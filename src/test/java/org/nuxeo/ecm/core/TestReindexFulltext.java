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

import static org.junit.Assert.assertEquals;

import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.VersioningOption;
import org.nuxeo.ecm.core.api.impl.DocumentModelImpl;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(cleanup = Granularity.METHOD)
public class TestReindexFulltext {

    @Inject
    protected CoreSession session;

    @Test
    public void testReindexFulltext() throws Exception {
        // create a live doc
        DocumentModel file = session.createDocumentModel("/", "file", "File");
        file = session.createDocument(file);

        // create a version
        DocumentRef ver = session.checkIn(file.getRef(), VersioningOption.MINOR, null);

        // create a proxy (not reindexed)
        session.createProxy(ver, session.getRootDocument().getRef());

        // create an unfiled doc
        DocumentModel file2 = session.createDocumentModel((String) null, "file2", "File");
        session.createDocument(file2);

        session.save();

        ReindexFulltextRoot reindex = new ReindexFulltextRoot();
        reindex.session = session;
        String ok = reindex.reindexFulltext(0, 0);
        assertEquals("done: 3 total: 3 batch_errors: 0", ok);
    }

}
