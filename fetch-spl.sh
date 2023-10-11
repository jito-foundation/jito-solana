#!/usr/bin/env bash
#
# Fetches the latest SPL programs and produces the solana-genesis command-line
# arguments needed to install them
#

set -e

upgradeableLoader=BPFLoaderUpgradeab1e11111111111111111111111

fetch_program() {
  declare name=$1
  declare version=$2
  declare address=$3
  declare loader=$4
  declare repo=$5

  case $repo in
  "jito")
    so=$name-$version.so
    so_name="$name.so"
    url="https://github.com/jito-foundation/jito-programs/releases/download/v$version/$so_name"
    ;;
  "solana")
    so=spl_$name-$version.so
    so_name="spl_${name//-/_}.so"
    url="https://github.com/solana-labs/solana-program-library/releases/download/$name-v$version/$so_name"
    ;;
  *)
    echo "Unsupported repo: $repo"
    return 1
    ;;
  esac

  if [[ $loader == "$upgradeableLoader" ]]; then
    genesis_args+=(--upgradeable-program "$address" "$loader" "$so" none)
  else
    genesis_args+=(--bpf-program "$address" "$loader" "$so")
  fi

  if [[ -r $so ]]; then
    return
  fi

  if [[ -r ~/.cache/solana-spl/$so ]]; then
    cp ~/.cache/solana-spl/"$so" "$so"
  else
    echo "Downloading $name $version"
    (
      set -x
      curl -L --retry 5 --retry-delay 2 --retry-connrefused \
        -o "$so" \
        "$url"
    )

    mkdir -p ~/.cache/solana-spl
    cp "$so" ~/.cache/solana-spl/"$so"
  fi

}

fetch_program token 3.5.0 TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA BPFLoader2111111111111111111111111111111111 solana
fetch_program token-2022 0.6.0 TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb BPFLoaderUpgradeab1e11111111111111111111111 solana
fetch_program memo 1.0.0 Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo BPFLoader1111111111111111111111111111111111 solana
fetch_program memo 3.0.0 MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr BPFLoader2111111111111111111111111111111111 solana
fetch_program associated-token-account 1.1.2 ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL BPFLoader2111111111111111111111111111111111 solana
fetch_program feature-proposal 1.0.0 Feat1YXHhH6t1juaWF74WLcfv4XoNocjXA6sPWHNgAse BPFLoader2111111111111111111111111111111111 solana
# jito programs
fetch_program jito_tip_payment 0.1.4 T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt BPFLoaderUpgradeab1e11111111111111111111111 jito
fetch_program jito_tip_distribution 0.1.4 4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7 BPFLoaderUpgradeab1e11111111111111111111111 jito

echo "${genesis_args[@]}" >spl-genesis-args.sh

echo
echo "Available SPL programs:"
ls -l spl_*.so

echo "Available Jito programs:"
ls -l jito*.so

echo
echo "solana-genesis command-line arguments (spl-genesis-args.sh):"
cat spl-genesis-args.sh
