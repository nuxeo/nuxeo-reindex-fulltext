/*
 * (C) Copyright 2012 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.IterableQueryResult;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.api.repository.FulltextConfiguration;
import org.nuxeo.ecm.core.event.EventService;
import org.nuxeo.ecm.core.model.Repository;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.ecm.core.repository.RepositoryService;
import org.nuxeo.ecm.core.storage.FulltextExtractorWork;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.core.work.api.WorkManager;
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

    private static Log log = LogFactory.getLog(ReindexFulltextRoot.class);

    protected static final int DEFAULT_BATCH_SIZE = 100;

    @Context
    protected HttpServletRequest request;

    protected CoreSession session;

    protected FulltextConfiguration fulltextConfiguration;

    @GET
    public String get(@QueryParam("batchSize") int batchSize, @QueryParam("batch") int batch) {
        session = SessionFactory.getSession(request);
        return reindexFulltext(batchSize, batch);
    }

    /**
     * Launches a fulltext reindexing of the database.
     *
     * @param batchSize the batch size, defaults to 100
     * @param batch if present, the batch number to process instead of all batches; starts at 1
     * @return when done, ok + the total number of docs
     */
    public String reindexFulltext(int batchSize, int batch) {
        RepositoryService repositoryService = Framework.getService(RepositoryService.class);
        Repository repository = repositoryService.getRepository(session.getRepositoryName());
        fulltextConfiguration = repository.getFulltextConfiguration();

        if (!session.getPrincipal().isAdministrator()) {
            return "unauthorized";
        }

        log("Reindexing starting");
        if (batchSize <= 0) {
            batchSize = DEFAULT_BATCH_SIZE;
        }
        List<String> allIds = getAllIds();
        int size = allIds.size();
        int numBatches = (size + batchSize - 1) / batchSize;
        if (batch < 0 || batch > numBatches) {
            batch = 0; // all
        }
        batch--;

        log("Reindexing of %s documents, batch size: %s, number of batches: %s", size, batchSize, numBatches);
        if (batch >= 0) {
            log("Reindexing limited to batch: %s", batch + 1);
        }

        boolean tx = TransactionHelper.isTransactionActiveOrMarkedRollback();
        if (tx) {
            TransactionHelper.commitOrRollbackTransaction();
        }
        EventService eventService = Framework.getService(EventService.class);

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
            List<String> batchIds = allIds.subList(pos, end);
            log("Reindexing batch %s/%s, first id: %s", i + 1, numBatches, batchIds.get(0));
            try {
                scheduleBatch(batchIds);
                eventService.waitForAsyncCompletion();
            } catch (NuxeoException e) {
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

    // TODO doesn't scale
    protected List<String> getAllIds() {
        List<String> ids = new ArrayList<>();
        String query = "SELECT ecm:uuid, ecm:primaryType FROM Document WHERE ecm:isProxy = 0 ORDER BY ecm:uuid";
        try (IterableQueryResult it = session.queryAndFetch(query, NXQL.NXQL)) {
            for (Map<String, Serializable> map : it) {
                String type = (String) map.get(NXQL.ECM_PRIMARYTYPE);
                if (fulltextConfiguration.isFulltextIndexable(type)) {
                    ids.add((String) map.get(NXQL.ECM_UUID));
                }
            }
        }
        return ids;
    }

    protected void scheduleBatch(List<String> ids) {
        String repositoryName = session.getRepositoryName();
        WorkManager workManager = Framework.getService(WorkManager.class);
        for (String id : ids) {
            Work work = new FulltextExtractorWork(repositoryName, id, true, true, false); // no job id
            // schedule immediately, we're outside a transaction
            workManager.schedule(work, false);
        }
    }

}
