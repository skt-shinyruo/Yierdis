## MODIFIED Requirements

### Requirement: Off-heap layer is packaged as separate modules
The system SHALL package the off-heap layer as separate Maven modules/jars for API and backend implementations.

#### Scenario: Default build includes API + Netty backend
- **WHEN** a developer runs `mvn test` at the repo root
- **THEN** the reactor builds `yierdis-offheap` (pom) and its `api`/`netty` modules
- **AND** the server module builds against the API module

#### Scenario: Foreign backend remains optional
- **WHEN** a developer runs `mvn test` at the repo root
- **THEN** the build completes without compiling `yierdis-offheap-foreign`
- **AND** selecting the `foreign` backend at runtime fails with a clear error message

#### Scenario: Profile build compiles and tests foreign backend
- **WHEN** a developer runs `mvn -Pforeign-memory test`
- **THEN** the foreign backend module compiles with `--add-modules jdk.incubator.foreign`
- **AND** the foreign backend unit tests run and pass
