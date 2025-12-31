## ADDED Requirements

### Requirement: Foreign backend is a separate module
The system SHALL package the Foreign Memory backend as a separate Maven module/jar rather than an extra conditional source root.

#### Scenario: Default build does not compile incubator code
- **WHEN** a developer runs `mvn test` at the repo root
- **THEN** the build completes on Java 17 without `--add-modules jdk.incubator.foreign`

#### Scenario: Profile build compiles and tests foreign backend
- **WHEN** a developer runs `mvn -Pforeign-memory test`
- **THEN** the foreign backend module compiles with `--add-modules jdk.incubator.foreign`
- **AND** the foreign backend unit tests run and pass

### Requirement: Off-heap API is reusable across modules
The system SHALL package the off-heap API and the Netty backend as a separate Maven module/jar so that optional backends can depend on it
without introducing circular dependencies.

#### Scenario: Server builds without foreign module
- **WHEN** a developer runs `mvn test` at the repo root
- **THEN** the server module builds and tests without building the foreign backend module
