# blockchain-dbt

## Install

```
$ pip install -u git+https://github.com/datawaves-xyz/blockchain-dbt.git@master
```

## Codegen

- dbt repo: [dbt_ethereum_source](https://github.com/datawaves-xyz/dbt_ethereum_source)

```
$ git clone https://github.com/datawaves-xyz/dbt_ethereum_source
$ cd dbt_ethereum_source
$ bdbt ethereum_codegen
```

## Export NFT metadata

```
$ bdbt export_added_nft_metadata \
  --api-keys YOUR_KEYS \
  --contract-address 0xffbe1944d868bb909cbade27674dd3670472ead4,0xffce5f9b3ef3ea9ab68591ea268d36c8f216bd02, \
  --output-prefix "nft-metadata"
```