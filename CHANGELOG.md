# Changelog

All notable changes to Sparvi Core will be documented in this file.

## [0.5.1] - 2025-03-09
### Changed
- Version bump

## [0.5.0] - 2025-03-09
### Added
- Snowflake support as the default database adapter
- Snowflake-specific SQL optimizations for better performance
- Environment variable integration for Snowflake credentials
- New examples demonstrating Snowflake usage patterns

### Changed
- Modified default connection string to use Snowflake format
- Updated installation requirements to include snowflake-sqlalchemy by default
- Optimized profiling queries specifically for Snowflake architecture
- Improved adapter selection to prioritize Snowflake connections

### Fixed
- SQL dialect handling for Snowflake regex patterns
- Date and time functions adjusted for Snowflake syntax

## [0.4.2] - 2025-03-05
### Fixed
- Fixed profile_table queries

## [0.4.1] - 2025-03-04
### Fixed
- Fixed URL in documentation

### Changed
- Updated profile engine to default to no row level samples returned

## [0.4.0] - 2025-03-04
### Added
- Initial release of Sparvi Core package
- CLI for profile and validation commands
- Profile engine for data profiling
- Validation framework with default rules