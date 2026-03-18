# Plan: Package IOMETE Fork of Arrow Flight JDBC Driver

## Context

IOMETE maintains a fork of Apache Arrow Java with a gRPC proxy detector feature (`grpc-proxy-detector` branch). The fork adds HTTP CONNECT proxy tunneling support to the Flight SQL JDBC driver. We need to produce a distributable fat JAR that is identifiable as IOMETE's fork, distinct from the official Apache Arrow distribution.

**Decisions made:**
- Publish only the fat JAR (`flight-sql-jdbc-driver`) — single dependency for consumers
- Keep `org.apache.arrow` groupId (drop-in replacement)
- Version: `19.0.0-iomete.1` (dot-separated, encodes upstream version + IOMETE qualifier + patch)
- Keep original Java package names (`org.apache.arrow.driver.jdbc.*`) for easy rebase
- Local build only for now; artifact manager TBD

## Risk Mitigation (keeping `org.apache.arrow` groupId)

Since we're keeping the official groupId, we must prevent accidental resolution conflicts:
- **Never publish to Maven Central** under `org.apache.arrow` without Apache PMC approval
- When publishing to a private repo, ensure it takes priority over Maven Central in consumer `settings.xml`
- The version `19.0.0-iomete.1` is distinct enough to avoid accidental resolution against official `19.0.0`

## Steps

### Step 1: Set version to `19.0.0-iomete.1`

Use Maven's `versions:set` to change the version across all modules atomically:

```bash
mise exec -- mvn versions:set -DnewVersion=19.0.0-iomete.1 -DgenerateBackupPoms=false
```

This updates the root POM and all child modules that inherit from it. The fat JAR will be produced as `flight-sql-jdbc-driver-19.0.0-iomete.1.jar`.

**Files affected:** All `pom.xml` files (version inheritance). No manual edits needed.

### Step 2: Build the entire project

The fat JAR module (`flight-sql-jdbc-driver`) depends on several sibling modules. We must build from root:

```bash
mise exec -- mvn install -DskipTests -Dcheckstyle.skip=true -Dspotless.check.skip=true
```

- `-DskipTests`: speed — tests were validated on the branch already
- `-Dcheckstyle.skip` + `-Dspotless.check.skip`: avoid style failures on generated/unmodified code

### Step 3: Verify the fat JAR

The shaded JAR will be at:
```
flight/flight-sql-jdbc-driver/target/flight-sql-jdbc-driver-19.0.0-iomete.1.jar
```

Verify it contains the proxy detector and correct metadata:
```bash
# Check JAR size (should be ~40-60MB, it's a fat JAR)
ls -lh flight/flight-sql-jdbc-driver/target/flight-sql-jdbc-driver-19.0.0-iomete.1.jar

# Verify proxy detector class is present
jar tf flight/flight-sql-jdbc-driver/target/flight-sql-jdbc-driver-19.0.0-iomete.1.jar | grep ProxyDetector

# Verify JDBC ServiceLoader registration
jar xf flight/flight-sql-jdbc-driver/target/flight-sql-jdbc-driver-19.0.0-iomete.1.jar META-INF/services/java.sql.Driver
cat META-INF/services/java.sql.Driver

# Check Maven metadata in JAR
jar tf flight/flight-sql-jdbc-driver/target/flight-sql-jdbc-driver-19.0.0-iomete.1.jar | grep pom.properties
```

### Step 4: (Optional) Add IOMETE manifest entry

Add a custom manifest attribute to the shade plugin config in `flight/flight-sql-jdbc-driver/pom.xml` for traceability:

```xml
<transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
  <manifestEntries>
    <Built-By>IOMETE</Built-By>
    <Implementation-Vendor>IOMETE</Implementation-Vendor>
    <Implementation-Version>19.0.0-iomete.1</Implementation-Version>
    <X-Fork-Source>Apache Arrow Java 19.0.0</X-Fork-Source>
  </manifestEntries>
</transformer>
```

**File:** `flight/flight-sql-jdbc-driver/pom.xml` (shade plugin `<transformers>` section)

### Step 5: Commit the version change

Commit on the `grpc-proxy-detector` branch with a clear message indicating the IOMETE release version.

## Verification

1. Fat JAR exists at expected path with correct filename
2. `ProxyDetector` class is inside the JAR (grep confirms)
3. `META-INF/services/java.sql.Driver` points to `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver`
4. JAR can be loaded as a JDBC driver: `java -cp <jar> org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver` (no ClassNotFoundException)
5. `pom.properties` inside JAR shows `version=19.0.0-iomete.1`

## Future Considerations (not in scope now)

- **Artifact manager**: When ready, add `<distributionManagement>` to root POM pointing to GitHub Packages / Nexus / Artifactory
- **CI/CD**: GitHub Actions workflow to build + publish on git tag `v19.0.0-iomete.*`
- **Upstream rebase**: When Arrow releases 20.0.0, rebase branch, bump to `20.0.0-iomete.1`
