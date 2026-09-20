# Changelog

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
