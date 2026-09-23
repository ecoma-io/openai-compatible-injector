# Changelog

## [0.10.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.9.0...v0.10.0) (2026-09-23)


### ⚠ BREAKING CHANGES

* **config:** align recovery ownership, override scope and failure evidence ([#59](https://github.com/ecoma-io/openai-compatible-injector/issues/59))

### Features

* **config:** align recovery ownership, override scope and failure evidence ([#59](https://github.com/ecoma-io/openai-compatible-injector/issues/59)) ([49cad18](https://github.com/ecoma-io/openai-compatible-injector/commit/49cad189eb6a0cff47efdc6aace86bc4f9401f27))

## [0.9.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.8.0...v0.9.0) (2026-09-23)


### ⚠ BREAKING CHANGES

* **proxy:** recover through a configurable policy engine ([#56](https://github.com/ecoma-io/openai-compatible-injector/issues/56))

### Features

* **proxy:** recover through a configurable policy engine ([#56](https://github.com/ecoma-io/openai-compatible-injector/issues/56)) ([4dd9a49](https://github.com/ecoma-io/openai-compatible-injector/commit/4dd9a49eb8a4ea0f0cae6b3ca9d6786b7f8478b2))

## [0.8.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.7.0...v0.8.0) (2026-09-23)


### ⚠ BREAKING CHANGES

* **proxy:** status-aware per-candidate retries and provider fallback ([#53](https://github.com/ecoma-io/openai-compatible-injector/issues/53))

### Features

* **proxy:** status-aware per-candidate retries and provider fallback ([#53](https://github.com/ecoma-io/openai-compatible-injector/issues/53)) ([371e66c](https://github.com/ecoma-io/openai-compatible-injector/commit/371e66c2316eb5e2aeb92eaa68b3accdc768b742))

## [0.7.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.6.0...v0.7.0) (2026-09-23)


### Features

* **proxy:** add multi-egress transport routing ([#41](https://github.com/ecoma-io/openai-compatible-injector/issues/41)) ([f9261ac](https://github.com/ecoma-io/openai-compatible-injector/commit/f9261ac7e69d231f1505555eddc4c24017458b81))
* **proxy:** add provider transport abstraction ([#39](https://github.com/ecoma-io/openai-compatible-injector/issues/39)) ([138b5b4](https://github.com/ecoma-io/openai-compatible-injector/commit/138b5b4aaad806cece019d24a0298cca87220ced))
* **proxy:** egress scheduling, provider fallback chains, partner keys, usage metering ([#48](https://github.com/ecoma-io/openai-compatible-injector/issues/48)) ([098a8a2](https://github.com/ecoma-io/openai-compatible-injector/commit/098a8a285072109ad7c3ac2130edf6a83678e036))


### Bug Fixes

* **proxy:** caller-deadline fallback, bounded error capture, canonical failure evidence ([#51](https://github.com/ecoma-io/openai-compatible-injector/issues/51)) ([61789ce](https://github.com/ecoma-io/openai-compatible-injector/commit/61789cebcafa0b46396f8b99fcac0fb908e9708a)), closes [#49](https://github.com/ecoma-io/openai-compatible-injector/issues/49) [#50](https://github.com/ecoma-io/openai-compatible-injector/issues/50)
* **proxy:** normalize upstream HTTP 4xx/5xx into a canonical envelope ([#36](https://github.com/ecoma-io/openai-compatible-injector/issues/36)) ([269102e](https://github.com/ecoma-io/openai-compatible-injector/commit/269102ed8d8005fe5a446c304929a11f518b6927))

## [0.6.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.5.0...v0.6.0) (2026-09-22)


### Features

* **proxy:** inject SSE keep-alive comments during upstream silence ([#32](https://github.com/ecoma-io/openai-compatible-injector/issues/32)) ([38edc95](https://github.com/ecoma-io/openai-compatible-injector/commit/38edc95826c8e64bc9121f092e029a36077ef0ba))

## [0.5.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.4.0...v0.5.0) (2026-09-22)


### ⚠ BREAKING CHANGES

* **config:** require client api-key and stop forwarding Authorization upstream ([#28](https://github.com/ecoma-io/openai-compatible-injector/issues/28))
* **config:** replace logging.level section with top-level log-level key ([#25](https://github.com/ecoma-io/openai-compatible-injector/issues/25))

### Features

* **config:** replace logging.level section with top-level log-level key ([#25](https://github.com/ecoma-io/openai-compatible-injector/issues/25)) ([11d5715](https://github.com/ecoma-io/openai-compatible-injector/commit/11d571549a5515795a5f690a02e7fd7491d9ff33))
* **config:** require client api-key and stop forwarding Authorization upstream ([#28](https://github.com/ecoma-io/openai-compatible-injector/issues/28)) ([f0cb1de](https://github.com/ecoma-io/openai-compatible-injector/commit/f0cb1de6521b5ef95a12b65bf7653dbf30a629a5))

## [0.4.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.3.0...v0.4.0) (2026-09-21)


### ⚠ BREAKING CHANGES

* **config:** read only OAICR_-prefixed bootstrap env vars ([#21](https://github.com/ecoma-io/openai-compatible-injector/issues/21))

### Features

* **config:** read only OAICR_-prefixed bootstrap env vars ([#21](https://github.com/ecoma-io/openai-compatible-injector/issues/21)) ([48bb953](https://github.com/ecoma-io/openai-compatible-injector/commit/48bb95316763329c2250fa0dc11e9b099f0764f0))
* **workspace:** default VERSION build-arg to dev, drop compose build args ([#22](https://github.com/ecoma-io/openai-compatible-injector/issues/22)) ([0733470](https://github.com/ecoma-io/openai-compatible-injector/commit/0733470b927fb765b2afadb2231d631393699887))

## [0.3.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.2.0...v0.3.0) (2026-09-21)


### Features

* **config:** per-model thinking-usage block ([#16](https://github.com/ecoma-io/openai-compatible-injector/issues/16)) ([b77fde7](https://github.com/ecoma-io/openai-compatible-injector/commit/b77fde77b4dd19d78855bc5108f6f044921d5e78))
* simulated thinking-usage synthesis (inject + proxy + e2e) ([#17](https://github.com/ecoma-io/openai-compatible-injector/issues/17)) ([4412632](https://github.com/ecoma-io/openai-compatible-injector/commit/4412632d312a4267173fe4343fd3dcb1b1644de3))

## [0.2.0](https://github.com/ecoma-io/openai-compatible-injector/compare/v0.1.0...v0.2.0) (2026-09-20)


### Features

* production-readiness pass — bounded SSE, per-API rewrite, outcome fidelity, observable reload ([#11](https://github.com/ecoma-io/openai-compatible-injector/issues/11)) ([b26576f](https://github.com/ecoma-io/openai-compatible-injector/commit/b26576f47411f8cf74234ba0769ad7825649deed))

## 0.1.0 (2026-09-20)


### Features

* **cmd:** add CLI entrypoint with black-box e2e suite ([b5ddd99](https://github.com/ecoma-io/openai-compatible-injector/commit/b5ddd991cb8e98910018e24de575df0ee203330e))
* **config:** add two-plane configuration with hot reload ([22ca8ab](https://github.com/ecoma-io/openai-compatible-injector/commit/22ca8abca1ff096698b8b2a008e271660dec75c5))
* hardening, observability, and performance pass ([#7](https://github.com/ecoma-io/openai-compatible-injector/issues/7)) ([a0afebe](https://github.com/ecoma-io/openai-compatible-injector/commit/a0afebe59507407de9c979255f57b74ea45d29d6))
* implement OpenAI-compatible injector ([baf211f](https://github.com/ecoma-io/openai-compatible-injector/commit/baf211fa51e1db130b9f9d6cbed248ef908bb6c9))
* **inject:** rewrite models and inject prompts for both APIs ([fff6bed](https://github.com/ecoma-io/openai-compatible-injector/commit/fff6bed9de054cb7ccb13ec5710ad1c46a8fe2cd))
* **proxy:** add OpenAI-compatible handler with SSE passthrough ([4d27a28](https://github.com/ecoma-io/openai-compatible-injector/commit/4d27a28f5d8ca2fa351a2cdc5845be2fce1aa739))


### Bug Fixes

* **cmd:** make second-signal force-exit unconditional ([d668b2d](https://github.com/ecoma-io/openai-compatible-injector/commit/d668b2d5c83f8c7e362803fbf045167052f3fa87))
* **config:** decode runtime with strict yaml.v3, drop viper ([82dfa46](https://github.com/ecoma-io/openai-compatible-injector/commit/82dfa46420920d1047ae6329defb998baf817614))
* **proxy:** avoid overflow-prone allocation size in SSE rewrite ([10c0aef](https://github.com/ecoma-io/openai-compatible-injector/commit/10c0aef289f9a803f87574e8bcf9446a6be03fca))
* **release:** keep initial-version at 0.1.0 for the first release ([#9](https://github.com/ecoma-io/openai-compatible-injector/issues/9)) ([12f1365](https://github.com/ecoma-io/openai-compatible-injector/commit/12f1365000125c1cd19a990a9afd68cde02cc195))


### Documentation

* add deployment, CI, and community files ([8cdea22](https://github.com/ecoma-io/openai-compatible-injector/commit/8cdea22114a12fadcd1148533f8410db403fe8a6))
* describe yaml.v3 strict decode after viper removal ([#5](https://github.com/ecoma-io/openai-compatible-injector/issues/5)) ([4cdfb6a](https://github.com/ecoma-io/openai-compatible-injector/commit/4cdfb6a06e5fc59d0c1c093160fba153e5a12ce7)), closes [#4](https://github.com/ecoma-io/openai-compatible-injector/issues/4)
* pin exact bytes for both 400 envelopes ([a34a6cd](https://github.com/ecoma-io/openai-compatible-injector/commit/a34a6cd814688753036d3544b3c8e05c29edd672))
