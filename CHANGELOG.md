# Changelog
All notable changes to 'fty-messagebus2' project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

[1.0.X] - 2022-mm-dd [IN PROGRESS]
### Added
- Add secured class for promise/future management.
### Changed
- Reworking Amqp code.
### Removed
- Remove the intermediate class MsgBusAmqp.
### Fixed

[1.0.1] - 2022-07-13
### Added
- Add callback for definitive communication lost treatment.
### Changed
- Reworking Amqp code.
- Optimise the number of simultaneous connection on the Amqp library.
- Modify name space of the project.
### Fixed
- Close missing open handles.
- Modify default configuration to avoid loss of connection.

[1.0.0] - 2022-01-20
### Added
- First version
