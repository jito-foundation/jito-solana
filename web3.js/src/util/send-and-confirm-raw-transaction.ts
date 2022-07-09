import type {Buffer} from 'buffer';

import {
  BlockheightBasedTransactionConfirmationStrategy,
  Connection,
} from '../connection';
import type {TransactionSignature} from '../transaction';
import type {ConfirmOptions} from '../connection';

/**
 * Send and confirm a raw transaction
 *
 * If `commitment` option is not specified, defaults to 'max' commitment.
 *
 * @param {Connection} connection
 * @param {Buffer} rawTransaction
 * @param {BlockheightBasedTransactionConfirmationStrategy} confirmationStrategy
 * @param {ConfirmOptions} [options]
 * @returns {Promise<TransactionSignature>}
 */
export async function sendAndConfirmRawTransaction(
  connection: Connection,
  rawTransaction: Buffer,
  confirmationStrategy: BlockheightBasedTransactionConfirmationStrategy,
  options?: ConfirmOptions,
): Promise<TransactionSignature>;

/**
 * @deprecated Calling `sendAndConfirmRawTransaction()` without a `confirmationStrategy`
 * is no longer supported and will be removed in a future version.
 */
// eslint-disable-next-line no-redeclare
export async function sendAndConfirmRawTransaction(
  connection: Connection,
  rawTransaction: Buffer,
  options?: ConfirmOptions,
): Promise<TransactionSignature>;

// eslint-disable-next-line no-redeclare
export async function sendAndConfirmRawTransaction(
  connection: Connection,
  rawTransaction: Buffer,
  confirmationStrategyOrConfirmOptions:
    | BlockheightBasedTransactionConfirmationStrategy
    | ConfirmOptions
    | undefined,
  maybeConfirmOptions?: ConfirmOptions,
): Promise<TransactionSignature> {
  let confirmationStrategy:
    | BlockheightBasedTransactionConfirmationStrategy
    | undefined;
  let options: ConfirmOptions | undefined;
  if (
    confirmationStrategyOrConfirmOptions &&
    Object.prototype.hasOwnProperty.call(
      confirmationStrategyOrConfirmOptions,
      'lastValidBlockHeight',
    )
  ) {
    confirmationStrategy =
      confirmationStrategyOrConfirmOptions as BlockheightBasedTransactionConfirmationStrategy;
    options = maybeConfirmOptions;
  } else {
    options = confirmationStrategyOrConfirmOptions as
      | ConfirmOptions
      | undefined;
  }
  const sendOptions = options && {
    skipPreflight: options.skipPreflight,
    preflightCommitment: options.preflightCommitment || options.commitment,
    minContextSlot: options.minContextSlot,
  };

  const signature = await connection.sendRawTransaction(
    rawTransaction,
    sendOptions,
  );

  const commitment = options && options.commitment;
  const confirmationPromise = confirmationStrategy
    ? connection.confirmTransaction(confirmationStrategy, commitment)
    : connection.confirmTransaction(signature, commitment);
  const status = (await confirmationPromise).value;

  if (status.err) {
    throw new Error(
      `Raw transaction ${signature} failed (${JSON.stringify(status)})`,
    );
  }

  return signature;
}
