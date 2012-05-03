/*
 * (C) Copyright 2012 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.AbstractSession;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentException;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.IdRef;
import org.nuxeo.ecm.core.api.IterableQueryResult;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.core.api.TransactionalCoreSessionWrapper;
import org.nuxeo.ecm.core.event.Event;
import org.nuxeo.ecm.core.event.EventContext;
import org.nuxeo.ecm.core.event.EventService;
import org.nuxeo.ecm.core.event.impl.EventContextImpl;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.ecm.core.storage.sql.Model;
import org.nuxeo.ecm.core.storage.sql.ModelFulltext;
import org.nuxeo.ecm.core.storage.sql.Session;
import org.nuxeo.ecm.core.storage.sql.coremodel.BinaryTextListener;
import org.nuxeo.ecm.core.storage.sql.coremodel.SQLDocument;
import org.nuxeo.ecm.core.storage.sql.coremodel.SQLSession;
import org.nuxeo.ecm.webengine.jaxrs.session.SessionFactory;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.transaction.TransactionHelper;

/**
 * JAX-RS component used to do fulltext reindexing of the whole database.
 *
 * @since 5.6
 */
@Path("reindexFulltext")
public class ReindexFulltextRoot {

    public static Log log = LogFactory.getLog(ReindexFulltextRoot.class);

    // org.nuxeo.ecm.platform.dublincore.listener.DublinCoreListener.DISABLE_DUBLINCORE_LISTENER
    protected static final String DISABLE_DUBLINCORE_LISTENER = "disableDublinCoreListener";

    // org.nuxeo.ecm.platform.ec.notification.NotificationConstants.DISABLE_NOTIFICATION_SERVICE
    protected static final String DISABLE_NOTIFICATION_SERVICE = "disableNotificationService";

    protected static final String DC_TITLE = "dc:title";

    protected static final int DEFAULT_BATCH_SIZE = 100;

    @Context
    protected HttpServletRequest request;

    protected CoreSession session;

    protected ModelFulltext fulltextInfo;

    protected static class Info {
        public final String id;

        public final String type;

        public Info(String id, String type) {
            this.id = id;
            this.type = type;
        }
    }

    /**
     * Launches a fulltext reindexing of the database.
     *
     * @param batchSize the batch size, defaults to 100
     * @param batch if present, the batch number to process instead of all
     *            batches; starts at 1
     * @return when done, ok + the total number of docs
     */
    @GET
    public String reindexFulltext(@QueryParam("batchSize") int batchSize,
            @QueryParam("batch") int batch) throws Exception {
        session = SessionFactory.getSession(request);
        Principal principal = session.getPrincipal();
        if (!(principal instanceof NuxeoPrincipal)) {
            return "unauthorized";
        }
        NuxeoPrincipal nuxeoPrincipal = (NuxeoPrincipal) principal;
        if (!nuxeoPrincipal.isAdministrator()) {
            return "unauthorized";
        }
        fulltextInfo = getFulltextInfo();

        log("Reindexing starting");
        if (batchSize <= 0) {
            batchSize = DEFAULT_BATCH_SIZE;
        }
        List<Info> infos = getInfos();
        int size = infos.size();
        int numBatches = (size + batchSize - 1) / batchSize;
        if (batch < 0 || batch > numBatches) {
            batch = 0; // all
        }
        batch--;

        log("Reindexing of %s documents, batch size: %s, number of batches: %s",
                size, batchSize, numBatches);
        if (batch >= 0) {
            log("Reindexing limited to batch: %s", batch + 1);
        }

        boolean tx = TransactionHelper.isTransactionActive();
        if (tx) {
            TransactionHelper.commitOrRollbackTransaction();
        }

        int n = 0;
        int errs = 0;
        for (int i = 0; i < numBatches; i++) {
            if (batch >= 0 && batch != i) {
                continue;
            }
            int pos = i * batchSize;
            int end = pos + batchSize;
            if (end > size) {
                end = size;
            }
            List<Info> batchInfos = infos.subList(pos, end);
            log("Reindexing batch %s/%s, first id: %s", i + 1, numBatches,
                    batchInfos.get(0).id);
            try {
                doBatch(batchInfos);
            } catch (ClientException e) {
                log.error("Error processing batch " + i + 1, e);
                errs++;
            }
            n += end - pos;
        }

        log("Reindexing done");
        if (tx) {
            TransactionHelper.startTransaction();
        }
        return "done: " + n + " total: " + size + " batch_errors: " + errs;
    }

    protected void log(String format, Object... args) {
        log.warn(String.format(format, args));
    }

    protected ModelFulltext getFulltextInfo() throws Exception {
        CoreSession coreSession;
        if (Proxy.isProxyClass(session.getClass())) {
            TransactionalCoreSessionWrapper w = (TransactionalCoreSessionWrapper) Proxy.getInvocationHandler(session);
            Field f1 = TransactionalCoreSessionWrapper.class.getDeclaredField("session");
            f1.setAccessible(true);
            coreSession = (CoreSession) f1.get(w);
        } else {
            coreSession = session;
        }

        SQLSession s = (SQLSession) ((AbstractSession) coreSession).getSession();
        Field f2 = SQLSession.class.getDeclaredField("session");
        f2.setAccessible(true);
        Session ss = (Session) f2.get(s);
        Model model = ss.getModel();
        return model.getFulltextInfo();
    }

    protected List<Info> getInfos() throws ClientException {
        List<Info> infos = new ArrayList<Info>();
        String query = "SELECT ecm:uuid, ecm:primaryType FROM Document"
                + " WHERE ecm:isProxy = 0"
                + " AND ecm:currentLifeCycleState <> 'deleted'"
                + " ORDER BY ecm:uuid";
        IterableQueryResult it = session.queryAndFetch(query, NXQL.NXQL);
        try {
            for (Map<String, Serializable> map : it) {
                String id = (String) map.get(NXQL.ECM_UUID);
                String type = (String) map.get(NXQL.ECM_PRIMARYTYPE);
                infos.add(new Info(id, type));
            }
        } finally {
            it.close();
        }
        return infos;
    }

    protected void doBatch(List<Info> infos) throws ClientException {
        Set<String> asyncIds;
        boolean ok;

        // transaction for the sync batch
        TransactionHelper.startTransaction();
        ok = false;
        try {
            asyncIds = runSyncBatch(infos);
            ok = true;
        } finally {
            if (!ok) {
                TransactionHelper.setTransactionRollbackOnly();
                log.error("Rolling back sync");
            }
            TransactionHelper.commitOrRollbackTransaction();
        }

        // transaction for the async batch firing (needs session)
        TransactionHelper.startTransaction();
        ok = false;
        try {
            runAsyncBatch(asyncIds);
            ok = true;
        } finally {
            if (!ok) {
                TransactionHelper.setTransactionRollbackOnly();
                log.error("Rolling back async fire");
            }
            TransactionHelper.commitOrRollbackTransaction();
        }
    }

    protected Set<String> runSyncBatch(List<Info> infos) throws ClientException {
        List<DocumentRef> refs = new ArrayList<DocumentRef>(infos.size());
        Set<String> asyncIds = new HashSet<String>();
        for (Info info : infos) {
            String id = info.id;
            IdRef ref = new IdRef(id);
            refs.add(ref);

            // mark async indexing to do
            if (!fulltextInfo.isFulltextIndexable(info.type)) {
                continue;
            }
            try {
                session.setDocumentSystemProp(ref,
                        SQLDocument.FULLTEXT_JOBID_SYS_PROP, id);
            } catch (DocumentException e) {
                log.error(e);
                continue;
            }
            asyncIds.add(id);
        }
        DocumentModel[] docs = session.getDocuments(
                refs.toArray(new DocumentRef[0])).toArray(new DocumentModel[0]);

        for (int i = 0; i < docs.length; i++) {
            DocumentModel doc = docs[i];
            String title = doc.getTitle();
            title += " "; // add space
            doc.setPropertyValue(DC_TITLE, title);
            docs[i] = saveDocument(doc);
        }
        session.save();

        for (int i = 0; i < docs.length; i++) {
            DocumentModel doc = docs[i];
            String title = doc.getTitle();
            if (title.endsWith(" ")) {
                title = title.substring(0, title.length() - 1); // remove space
            }
            doc.setPropertyValue(DC_TITLE, title);
            docs[i] = saveDocument(doc);
        }
        session.save();

        return asyncIds;
    }

    protected DocumentModel saveDocument(DocumentModel doc)
            throws ClientException {
        doc.putContextData(DISABLE_NOTIFICATION_SERVICE, Boolean.TRUE);
        doc.putContextData(DISABLE_DUBLINCORE_LISTENER, Boolean.TRUE);
        return session.saveDocument(doc);
    }

    protected void runAsyncBatch(Set<String> asyncIds) throws ClientException {
        if (asyncIds.isEmpty()) {
            return;
        }
        EventContext eventContext = new EventContextImpl(asyncIds, fulltextInfo);
        eventContext.setRepositoryName(session.getRepositoryName());
        Event event = eventContext.newEvent(BinaryTextListener.EVENT_NAME);
        EventService eventService = Framework.getLocalService(EventService.class);
        eventService.fireEvent(event);
        eventService.waitForAsyncCompletion();
    }

}
