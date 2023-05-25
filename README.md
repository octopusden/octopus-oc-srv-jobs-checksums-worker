Checksums/DLContents worker for registring different artifacts in PSQL database basing on *AMQP* queue messages.

## Limitations

*NXS* (maven-based) location type for registration is currently supported only.

## How to run:

-   Install with PIP:
    `python -m pip install oc-checksums-worker`
-   Provide all environment variables or arguments (see below)
-   Run as module:
    `python -m oc_checksums_worker.checksums_worker`


## Environment variables:

-   **AMQP_URL, AMQP_USER, AMQP_PASSWORD** - for queue connection (*RabbitMQ* or other *AMQP* implementation)
-   **MVN_URL, MVN_USER, MVN_PASSWORD** - for maven-like repository connection (*Sontatype Nexus* and *JFrog Artifactory* is currently supported only)
-   **PSQL_URL, PSQL_USER, PSQL_PASSWORD** - for *PSQL* database connection, used for Django models also.

**NOTE**: *PSQL_URL* should contan database schema as a parameter. Format:
*hostFQDN*:*port*/*instance*?search\_path=*schema*
Example:
`db.example.com:5432/test_instance?search_path=test_schema`
