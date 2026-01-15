.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

============================
Arrow Flight SQL JDBC Driver
============================

The Flight SQL JDBC driver is a JDBC driver implementation that uses
the :external+arrow:doc:`Flight SQL protocol <format/FlightSql>` under
the hood.  This driver can be used with any database that implements
Flight SQL.

Installation and Requirements
=============================

The driver is compatible with JDK 11+. Note that the following JVM
parameter is required:

.. code-block:: shell

   java --add-opens=java.base/java.nio=ALL-UNNAMED ...

To add a dependency via Maven, use a ``pom.xml`` like the following:

.. code-block:: xml

   <?xml version="1.0" encoding="UTF-8"?>
   <project xmlns="http://maven.apache.org/POM/4.0.0"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
     <modelVersion>4.0.0</modelVersion>
     <groupId>org.example</groupId>
     <artifactId>demo</artifactId>
     <version>1.0-SNAPSHOT</version>
     <properties>
       <arrow.version>18.1.0</arrow.version>
     </properties>
     <dependencies>
       <dependency>
         <groupId>org.apache.arrow</groupId>
         <artifactId>flight-sql-jdbc-driver</artifactId>
         <version>${arrow.version}</version>
       </dependency>
     </dependencies>
   </project>

Connecting to a Database
========================

The URI format is as follows::

  jdbc:arrow-flight-sql://HOSTNAME:PORT[/?param1=val1&param2=val2&...]

For example, take this URI::

  jdbc:arrow-flight-sql://localhost:12345/?username=admin&password=pass&useEncryption=1

This will connect to a Flight SQL service running on ``localhost`` on
port 12345.  It will create a secure, encrypted connection, and
authenticate using the username ``admin`` and the password ``pass``.

The components of the URI are as follows.

* The URI scheme must be ``jdbc:arrow-flight-sql://``.
* **HOSTNAME** is the hostname of the Flight SQL service.
* **PORT** is the port of the Flight SQL service.

Additional options can be passed as query parameters. Parameter names are
case-sensitive. The supported parameters are:

.. list-table::
   :header-rows: 1

   * - Parameter
     - Default
     - Description

   * - disableCertificateVerification
     - false
     - When TLS is enabled, whether to verify the server certificate

   * - password
     - null
     - The password for user/password authentication

   * - threadPoolSize
     - 1
     - The size of an internal thread pool

   * - token
     - null
     - The token used for token authentication

   * - trustStore
     - null
     - When TLS is enabled, the path to the certificate store

   * - trustStorePassword
     - null
     - When TLS is enabled, the password for the certificate store

   * - tlsRootCerts
     - null
     - Path to PEM-encoded root certificates for TLS - use this as
       an alternative to ``trustStore``

   * - clientCertificate
     - null
     - Path to PEM-encoded client mTLS certificate when the Flight
       SQL server requires client verification.

   * - clientKey
     - null
     - Path to PEM-encoded client mTLS key when the Flight
       SQL server requires client verification.

   * - useEncryption
     - true
     - Whether to use TLS (the default is an encrypted connection)

   * - user
     - null
     - The username for user/password authentication

   * - useSystemTrustStore
     - true
     - When TLS is enabled, whether to use the system certificate store

   * - retainCookies
     - true
     - Whether to use cookies from the initial connection in subsequent
       internal connections when retrieving streams from separate endpoints.

   * - retainAuth
     - true
     - Whether to use bearer tokens obtained from the initial connection
       in subsequent internal connections used for retrieving streams
       from separate endpoints.

Note that URI values must be URI-encoded if they contain characters such
as !, @, $, etc.

Any URI parameters that are not handled by the driver are passed to
the Flight SQL service as gRPC headers. For example, the following URI ::

  jdbc:arrow-flight-sql://localhost:12345/?useEncryption=0&database=mydb

This will connect without authentication or encryption, to a Flight
SQL service running on ``localhost`` on port 12345. Each request will
also include a ``database=mydb`` gRPC header.

Connection parameters may also be supplied using the Properties object
when using the JDBC Driver Manager to connect. When supplying using
the Properties object, values should *not* be URI-encoded.

Parameters specified by the URI supercede parameters supplied by the
Properties object. When calling the `user/password overload of
DriverManager#getConnection()
<https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html#getConnection-java.lang.String-java.lang.String-java.lang.String->`_,
the username and password supplied on the URI supercede the username and
password arguments to the function call.

OAuth 2.0 Authentication
========================

The driver supports OAuth 2.0 authentication for obtaining access tokens
from an authorization server. Two OAuth flows are currently supported:

* **Client Credentials** - For service-to-service authentication where no
  user interaction is required. The application authenticates using its own
  credentials (client ID and client secret).

* **Token Exchange** (RFC 8693) - For exchanging one token for another,
  commonly used for federated authentication, delegation, or impersonation
  scenarios.

OAuth Connection Properties
---------------------------

The following properties configure OAuth authentication. These properties
should be provided via the ``Properties`` object when connecting, as they
may contain special characters that are difficult to encode in a URI.

**Common OAuth Properties**

.. list-table::
   :header-rows: 1

   * - Parameter
     - Type
     - Required
     - Default
     - Description

   * - oauth.flow
     - String
     - Yes (to enable OAuth)
     - null
     - The OAuth grant type. Supported values: ``client_credentials``,
       ``token_exchange``

   * - oauth.tokenUri
     - String
     - Yes
     - null
     - The OAuth 2.0 token endpoint URL (e.g.,
       ``https://auth.example.com/oauth/token``)

   * - oauth.clientId
     - String
     - Conditional
     - null
     - The OAuth 2.0 client ID. Required for ``client_credentials`` flow,
       optional for ``token_exchange``

   * - oauth.clientSecret
     - String
     - Conditional
     - null
     - The OAuth 2.0 client secret. Required for ``client_credentials`` flow,
       optional for ``token_exchange``

   * - oauth.scope
     - String
     - No
     - null
     - Space-separated list of OAuth scopes to request

   * - oauth.resource
     - String
     - No
     - null
     - The resource indicator for the token request (RFC 8707)

**Token Exchange Properties**

These properties are specific to the ``token_exchange`` flow:

.. list-table::
   :header-rows: 1

   * - Parameter
     - Type
     - Required
     - Default
     - Description

   * - oauth.exchange.subjectToken
     - String
     - Yes
     - null
     - The subject token to exchange (e.g., a JWT from an identity provider)

   * - oauth.exchange.subjectTokenType
     - String
     - Yes
     - null
     - The token type URI of the subject token. Common values:
       ``urn:ietf:params:oauth:token-type:access_token``,
       ``urn:ietf:params:oauth:token-type:jwt``

   * - oauth.exchange.actorToken
     - String
     - No
     - null
     - The actor token for delegation/impersonation scenarios

   * - oauth.exchange.actorTokenType
     - String
     - No
     - null
     - The token type URI of the actor token

   * - oauth.exchange.aud
     - String
     - No
     - null
     - The target audience for the exchanged token

   * - oauth.exchange.requestedTokenType
     - String
     - No
     - null
     - The desired token type for the exchanged token
