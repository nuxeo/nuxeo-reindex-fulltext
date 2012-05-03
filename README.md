# Nuxeo Reindex Fulltext

This is a simple plugin used to launch a full reindexing of the fulltext
of a Nuxeo repository.

Reindexing the fulltext is useful/needed if:

- you have changed your database fulltext analyzer configuration,
for instance from English to French,
- you have changed the available converters, for instance so that
new kinds of attached files can be converted to text,
- you have upgraded to Nuxeo 5.5 with PostgreSQL and followed
[this migration procedure](https://jira.nuxeo.com/browse/NXP-5689).

## Usage

    http://localhost:8080/nuxeo/site/reindexFulltext
    http://localhost:8080/nuxeo/site/reindexFulltext?batchSize=50
    http://localhost:8080/nuxeo/site/reindexFulltext?batchSize=50&batch=8
    http://localhost:8080/nuxeo/site/reindexFulltext?nxrepository=other

**batchSize**: the batch size (number of documents per transaction).
The default batch size is 100.

**batch**: if specified, only this specific batch will be processed.
Batch numbers start at 1.

**nxrepository**: if specified, the name of repository to reindex.
The default repository is "default".

When finished, the HTTP URL returns:

    done: 14 total: 14 batch_errors: 0

## Notes

If there is a HTTP timeout the processing will still continue on the server.

You can follow the progress in the logs, at WARN level:

    Reindexing starting
    Reindexing of 14 documents, batch size: 5, number of batches: 3
    Reindexing batch 1/3, first id: 159c64de-73ba-4d9e-a014-ff8ff4800d91
    Reindexing batch 2/3, first id: 7908cc07-8206-49fc-afbe-6d368f3226e9
    Reindexing batch 3/3, first id: d9686eef-b288-4d77-baf6-2ad7e4dd3ccf
    Reindexing done

Any errors in a batch will be logged and the transaction for this
batch rolled back, but the subsequent batches will be processed.

## About Nuxeo

Nuxeo provides a modular, extensible Java-based [open source software platform for enterprise content management] [1] and packaged applications for [document management] [2], [digital asset management] [3] and [case management] [4]. Designed by developers for developers, the Nuxeo platform offers a modern architecture, a powerful plug-in model and extensive packaging capabilities for building content applications.

[1]: http://www.nuxeo.com/en/products/ep
[2]: http://www.nuxeo.com/en/products/document-management
[3]: http://www.nuxeo.com/en/products/dam
[4]: http://www.nuxeo.com/en/products/case-management

More information on: <http://www.nuxeo.com/>
