# Changelog

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
