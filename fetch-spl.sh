#!/usr/bin/env bash
#
# Fetches the latest SPL programs and produces the solana-genesis command-line
# arguments needed to install them
#

set -e

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

source "$here"/fetch-programs.sh

PREFIX="spl"

programs=()

add_spl_program_to_fetch() {
  declare name=$1
  declare version=$2
  declare address=$3
  declare loader=$4
  declare repo=$5

  case $repo in
  "jito")
    so_name="$name.so"
    download_url="https://github.com/jito-foundation/jito-programs/releases/download/v$version/$so_name"
    ;;
  "solana")
    so_name="${PREFIX}_${name//-/_}.so"
    download_url="https://github.com/solana-program/$name/releases/download/program@v$version/$so_name"
    ;;
  *)
    echo "Unsupported repo: $repo"
    return 1
    ;;
  esac

  programs+=("$name $version $address $loader $download_url")
}

add_spl_program_to_fetch token 3.5.0 TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA BPFLoader2111111111111111111111111111111111 solana
add_spl_program_to_fetch token-2022 8.0.0 TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb BPFLoaderUpgradeab1e11111111111111111111111 solana
add_spl_program_to_fetch memo  1.0.0 Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo BPFLoader1111111111111111111111111111111111 solana
add_spl_program_to_fetch memo  3.0.0 MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr BPFLoader2111111111111111111111111111111111 solana
add_spl_program_to_fetch associated-token-account 1.1.2 ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL BPFLoader2111111111111111111111111111111111 solana
add_spl_program_to_fetch feature-proposal 1.0.0 Feat1YXHhH6t1juaWF74WLcfv4XoNocjXA6sPWHNgAse BPFLoader2111111111111111111111111111111111 solana
# jito programs
add_spl_program_to_fetch jito_tip_payment 0.1.10 T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt BPFLoaderUpgradeab1e11111111111111111111111 jito
add_spl_program_to_fetch jito_tip_distribution 0.1.10 4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7 BPFLoaderUpgradeab1e11111111111111111111111 jito

fetch_programs "$PREFIX" "${programs[@]}"