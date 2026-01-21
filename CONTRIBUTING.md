# Contributing to Shirath

Thank you for your interest in contributing to Shirath! This document provides guidelines and information for contributors.

## How to Contribute

### Reporting Bugs

Before creating a bug report, please check existing issues to avoid duplicates. When creating a bug report, include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected vs actual behavior
- Your environment (Elixir version, PostgreSQL version, ClickHouse version)
- Relevant logs or error messages

### Suggesting Features

Feature requests are welcome! Please provide:

- A clear description of the feature
- The problem it solves or use case it addresses
- Any implementation ideas you might have

### Pull Requests

1. Fork the repository
2. Create a feature branch from `master`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes
4. Write or update tests as needed
5. Ensure all tests pass:
   ```bash
   mix test
   ```
6. Format your code:
   ```bash
   mix format
   ```
7. Commit your changes with a descriptive message
8. Push to your fork and submit a pull request

## Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/shirath.git
   cd shirath
   ```

2. Install dependencies:
   ```bash
   mix deps.get
   ```

3. Copy and configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your local settings
   ```

4. Set up PostgreSQL with logical replication enabled (see README.md)

5. Set up ClickHouse (see README.md)

6. Create the publication on PostgreSQL:
   ```sql
   CREATE PUBLICATION shirath_dev FOR ALL TABLES;
   ```

7. Run the application:
   ```bash
   iex -S mix
   ```

## Code Style

- Follow standard Elixir conventions
- Use `mix format` before committing
- Write descriptive function and module documentation
- Keep functions small and focused
- Use pattern matching over conditional logic where appropriate

## Testing

- Write tests for new functionality
- Ensure existing tests pass before submitting PRs
- Use descriptive test names that explain what is being tested

```bash
# Run all tests
mix test

# Run specific test file
mix test test/path/to/test.exs

# Run with coverage
mix test --cover
```

## Commit Messages

- Use clear, descriptive commit messages
- Start with a verb in present tense (e.g., "Add", "Fix", "Update")
- Keep the first line under 72 characters
- Reference issues when applicable (e.g., "Fix #123")

Examples:
- `Add support for custom column transformations`
- `Fix connection pool exhaustion under high load`
- `Update documentation for mapper.json configuration`

## Questions?

Feel free to open an issue for any questions about contributing.
