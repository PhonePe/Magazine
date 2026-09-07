# Contributing to Magazine

Thank you for considering contributing to Magazine! This guide explains the process for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Reporting Issues](#reporting-issues)

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [opensource@phonepe.com](mailto:opensource@phonepe.com).

## Getting Started

1. **Fork** the repository on GitHub.
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/Magazine.git
   cd Magazine
   ```
3. **Add the upstream remote:**
   ```bash
   git remote add upstream https://github.com/PhonePe/Magazine.git
   ```

## Development Setup

### Prerequisites

- **Java 17** or later
- **Apache Maven 3.8+**
- **Docker** (integration tests use Testcontainers to run a real Aerospike server)

### Build

```bash
mvn clean install
```

### Run Tests

Always build the **full reactor** and use `verify`, not `test`:

```bash
mvn clean verify -Pcoverage
```

Two reasons this matters:

- `mvn test` does **not** run Javadoc. Broken `@link` references only surface under
  `verify`/`package`, and the build treats them as errors.
- Building `-pl magazine-dw-bundle` alone resolves a possibly stale `magazine-core` from your local
  repository, so bundle tests can pass against code you did not just change.

#### Docker discovery

Testcontainers only finds Docker automatically at the default socket. On macOS with Rancher
Desktop, Colima or Podman you must point it at the right one, or `AerospikeTestContainer` fails to
start:

```bash
# Rancher Desktop
export DOCKER_HOST=unix://$HOME/.rd/docker.sock

# Colima
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock

# Podman
export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
```

`export TESTCONTAINERS_RYUK_DISABLED=true` also helps if the Ryuk reaper cannot start in your
runtime.

The container requires the `NET_ADMIN` capability, which rootless Podman and some hardened Docker
configurations refuse. The first run also pulls the Aerospike image, so allow a few minutes.

### Architecture tests

`ArchitectureTest` enforces the package layering with ArchUnit — acyclic packages, Aerospike types
confined to `impl`, no Micrometer outside `metrics` and `impl`, and no DLM dependency returning.
These fail for non-obvious reasons, so read the rule's `because(...)` clause before working around
one.

### Coverage

`-Pcoverage` activates JaCoCo. The `magazine-coverage` module aggregates the per-module reports
into `magazine-coverage/target/site/jacoco-aggregate/`, which is what Sonar reads.

### Generate Javadoc

```bash
mvn javadoc:javadoc
```

## Making Changes

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/my-feature
   ```
2. Make your changes in small, focused commits.
3. Write or update tests for your changes.
4. Ensure all tests pass:
   ```bash
   mvn clean verify
   ```
5. Update documentation if your changes affect the public API.

## Pull Request Process

1. Push your branch to your fork:
   ```bash
   git push origin feature/my-feature
   ```
2. Open a Pull Request against the `main` branch of the upstream repository.
3. Fill in the PR template with:
   - A clear description of the change
   - The motivation / issue being addressed
   - Steps to test the change
4. Ensure the CI build passes.
5. Request review from at least one maintainer.
6. Address review feedback by pushing additional commits.
7. Once approved, a maintainer will merge your PR.

## Coding Standards

- **Language level:** Java 17 (use pattern matching, records, sealed classes where appropriate).
- **Formatting:** Follow the existing code style. Lombok annotations are used throughout—keep it consistent.
- **Naming:** Use clear, descriptive names. Prefix test methods with `test` or use descriptive `should_X_when_Y` naming.
- **Documentation:** Add Javadoc to all public classes and methods.
- **Testing:**
  - Unit tests with JUnit 5 and Mockito.
  - Integration tests with Testcontainers for backend-specific logic.
  - Aim for meaningful coverage; don't just chase numbers.
- **Dependencies:** Avoid adding new dependencies unless absolutely necessary. Discuss in the issue first.
- **Commits:** Write clear commit messages. Use the imperative mood ("Add feature" not "Added feature").

## Reporting Issues

- Use [GitHub Issues](https://github.com/PhonePe/Magazine/issues) to report bugs or request features.
- Include:
  - A clear title and description
  - Steps to reproduce (for bugs)
  - Expected vs. actual behavior
  - Magazine version, Java version, and backend version
  - Relevant logs or stack traces

## License

By contributing to Magazine, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
