import bs58 from 'bs58';
import BN from 'bn.js';
import * as mockttp from 'mockttp';

import {mockRpcMessage} from './rpc-websockets';
import {Connection, PublicKey, Transaction, Signer} from '../../src';
import invariant from '../../src/util/assert';
import type {Commitment, HttpHeaders, RpcParams} from '../../src/connection';

export const mockServer: mockttp.Mockttp | undefined =
  process.env.TEST_LIVE === undefined ? mockttp.getLocal() : undefined;

let uniqueCounter = 0;
export const uniqueSignature = () => {
  return bs58.encode(new BN(++uniqueCounter).toArray(undefined, 64));
};
export const uniqueBlockhash = () => {
  return bs58.encode(new BN(++uniqueCounter).toArray(undefined, 32));
};

export const mockErrorMessage = 'Invalid';
export const mockErrorResponse = {
  code: -32602,
  message: mockErrorMessage,
};

export const mockRpcBatchResponse = async ({
  batch,
  result,
  error,
}: {
  batch: RpcParams[];
  result: any[];
  error?: string;
}) => {
  if (!mockServer) return;

  const request = batch.map((batch: RpcParams) => {
    return {
      jsonrpc: '2.0',
      method: batch.methodName,
      params: batch.args,
    };
  });

  const response = result.map((result: any) => {
    return {
      jsonrpc: '2.0',
      id: '',
      result,
      error,
    };
  });

  await mockServer
    .post('/')
    .withJsonBodyIncluding(request)
    .thenReply(200, JSON.stringify(response));
};

export const mockRpcResponse = async ({
  method,
  params,
  value,
  error,
  withContext,
  withHeaders,
}: {
  method: string;
  params: Array<any>;
  value?: any;
  error?: any;
  withContext?: boolean;
  withHeaders?: HttpHeaders;
}) => {
  if (!mockServer) return;

  let result = value;
  if (withContext) {
    result = {
      context: {
        slot: 11,
      },
      value,
    };
  }

  await mockServer
    .post('/')
    .withJsonBodyIncluding({
      jsonrpc: '2.0',
      method,
      params,
    })
    .withHeaders(withHeaders || {})
    .thenReply(
      200,
      JSON.stringify({
        jsonrpc: '2.0',
        id: '',
        error,
        result,
      }),
    );
};

const latestBlockhash = async ({
  connection,
  commitment,
}: {
  connection: Connection;
  commitment?: Commitment;
}) => {
  const blockhash = uniqueBlockhash();
  const params = [];
  if (commitment) {
    params.push({commitment});
  }

  await mockRpcResponse({
    method: 'getLatestBlockhash',
    params,
    value: {
      blockhash,
      lastValidBlockHeight: 3090,
    },
    withContext: true,
  });

  return await connection.getLatestBlockhash(commitment);
};

const recentBlockhash = async ({
  connection,
  commitment,
}: {
  connection: Connection;
  commitment?: Commitment;
}) => {
  const blockhash = uniqueBlockhash();
  const params = [];
  if (commitment) {
    params.push({commitment});
  }

  await mockRpcResponse({
    method: 'getRecentBlockhash',
    params,
    value: {
      blockhash,
      feeCalculator: {
        lamportsPerSignature: 42,
      },
    },
    withContext: true,
  });

  return await connection.getRecentBlockhash(commitment);
};

const processTransaction = async ({
  connection,
  transaction,
  signers,
  commitment,
  err,
}: {
  connection: Connection;
  transaction: Transaction;
  signers: Array<Signer>;
  commitment: Commitment;
  err?: any;
}) => {
  const blockhash = (await latestBlockhash({connection})).blockhash;
  transaction.recentBlockhash = blockhash;
  transaction.sign(...signers);

  const encoded = transaction.serialize().toString('base64');
  invariant(transaction.signature);
  const signature = bs58.encode(transaction.signature);
  await mockRpcResponse({
    method: 'sendTransaction',
    params: [encoded],
    value: signature,
  });

  let sendOptions;
  if (err) {
    sendOptions = {
      skipPreflight: true,
    };
  } else {
    sendOptions = {
      preflightCommitment: commitment,
    };
  }

  await connection.sendEncodedTransaction(encoded, sendOptions);

  await mockRpcMessage({
    method: 'signatureSubscribe',
    params: [signature, {commitment}],
    result: {err: err || null},
  });
  await mockRpcMessage({
    method: 'signatureUnsubscribe',
    params: [1],
    result: true,
  });

  return await connection.confirmTransaction(signature, commitment);
};

const airdrop = async ({
  connection,
  address,
  amount,
}: {
  connection: Connection;
  address: PublicKey;
  amount: number;
}) => {
  await mockRpcResponse({
    method: 'requestAirdrop',
    params: [address.toBase58(), amount],
    value: uniqueSignature(),
  });

  const signature = await connection.requestAirdrop(address, amount);

  await mockRpcMessage({
    method: 'signatureSubscribe',
    params: [signature, {commitment: 'confirmed'}],
    result: {err: null},
  });
  await mockRpcMessage({
    method: 'signatureUnsubscribe',
    params: [1],
    result: true,
  });

  await connection.confirmTransaction(signature, 'confirmed');
  return signature;
};

export const helpers = {
  airdrop,
  processTransaction,
  recentBlockhash,
  latestBlockhash,
};
