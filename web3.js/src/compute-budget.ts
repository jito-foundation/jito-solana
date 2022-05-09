import * as BufferLayout from '@solana/buffer-layout';

import {
  encodeData,
  decodeData,
  InstructionType,
  IInstructionInputData,
} from './instruction';
import {PublicKey} from './publickey';
import {TransactionInstruction} from './transaction';

/**
 * Compute Budget Instruction class
 */
export class ComputeBudgetInstruction {
  /**
   * @internal
   */
  constructor() {}

  /**
   * Decode a compute budget instruction and retrieve the instruction type.
   */
  static decodeInstructionType(
    instruction: TransactionInstruction,
  ): ComputeBudgetInstructionType {
    this.checkProgramId(instruction.programId);

    const instructionTypeLayout = BufferLayout.u8('instruction');
    const typeIndex = instructionTypeLayout.decode(instruction.data);

    let type: ComputeBudgetInstructionType | undefined;
    for (const [ixType, layout] of Object.entries(
      COMPUTE_BUDGET_INSTRUCTION_LAYOUTS,
    )) {
      if (layout.index == typeIndex) {
        type = ixType as ComputeBudgetInstructionType;
        break;
      }
    }

    if (!type) {
      throw new Error(
        'Instruction type incorrect; not a ComputeBudgetInstruction',
      );
    }

    return type;
  }

  /**
   * Decode request units compute budget instruction and retrieve the instruction params.
   */
  static decodeRequestUnits(
    instruction: TransactionInstruction,
  ): RequestUnitsParams {
    this.checkProgramId(instruction.programId);
    const {units, additionalFee} = decodeData(
      COMPUTE_BUDGET_INSTRUCTION_LAYOUTS.RequestUnits,
      instruction.data,
    );
    return {units, additionalFee};
  }

  /**
   * Decode request heap frame compute budget instruction and retrieve the instruction params.
   */
  static decodeRequestHeapFrame(
    instruction: TransactionInstruction,
  ): RequestHeapFrameParams {
    this.checkProgramId(instruction.programId);
    const {bytes} = decodeData(
      COMPUTE_BUDGET_INSTRUCTION_LAYOUTS.RequestHeapFrame,
      instruction.data,
    );
    return {bytes};
  }

  /**
   * @internal
   */
  static checkProgramId(programId: PublicKey) {
    if (!programId.equals(ComputeBudgetProgram.programId)) {
      throw new Error(
        'invalid instruction; programId is not ComputeBudgetProgram',
      );
    }
  }
}

/**
 * An enumeration of valid ComputeBudgetInstructionType's
 */
export type ComputeBudgetInstructionType =
  // FIXME
  // It would be preferable for this type to be `keyof ComputeBudgetInstructionInputData`
  // but Typedoc does not transpile `keyof` expressions.
  // See https://github.com/TypeStrong/typedoc/issues/1894
  'RequestUnits' | 'RequestHeapFrame';

type ComputeBudgetInstructionInputData = {
  RequestUnits: IInstructionInputData & Readonly<RequestUnitsParams>;
  RequestHeapFrame: IInstructionInputData & Readonly<RequestHeapFrameParams>;
};

/**
 * Request units instruction params
 */
export interface RequestUnitsParams {
  /** Units to request for transaction-wide compute */
  units: number;

  /** Additional fee to pay */
  additionalFee: number;
}

/**
 * Request heap frame instruction params
 */
export type RequestHeapFrameParams = {
  /** Requested transaction-wide program heap size in bytes. Must be multiple of 1024. Applies to each program, including CPIs. */
  bytes: number;
};

/**
 * An enumeration of valid ComputeBudget InstructionType's
 * @internal
 */
export const COMPUTE_BUDGET_INSTRUCTION_LAYOUTS = Object.freeze<{
  [Instruction in ComputeBudgetInstructionType]: InstructionType<
    ComputeBudgetInstructionInputData[Instruction]
  >;
}>({
  RequestUnits: {
    index: 0,
    layout: BufferLayout.struct<
      ComputeBudgetInstructionInputData['RequestUnits']
    >([
      BufferLayout.u8('instruction'),
      BufferLayout.u32('units'),
      BufferLayout.u32('additionalFee'),
    ]),
  },
  RequestHeapFrame: {
    index: 1,
    layout: BufferLayout.struct<
      ComputeBudgetInstructionInputData['RequestHeapFrame']
    >([BufferLayout.u8('instruction'), BufferLayout.u32('bytes')]),
  },
});

/**
 * Factory class for transaction instructions to interact with the Compute Budget program
 */
export class ComputeBudgetProgram {
  /**
   * @internal
   */
  constructor() {}

  /**
   * Public key that identifies the Compute Budget program
   */
  static programId: PublicKey = new PublicKey(
    'ComputeBudget111111111111111111111111111111',
  );

  static requestUnits(params: RequestUnitsParams): TransactionInstruction {
    const type = COMPUTE_BUDGET_INSTRUCTION_LAYOUTS.RequestUnits;
    const data = encodeData(type, params);
    return new TransactionInstruction({
      keys: [],
      programId: this.programId,
      data,
    });
  }

  static requestHeapFrame(
    params: RequestHeapFrameParams,
  ): TransactionInstruction {
    const type = COMPUTE_BUDGET_INSTRUCTION_LAYOUTS.RequestHeapFrame;
    const data = encodeData(type, params);
    return new TransactionInstruction({
      keys: [],
      programId: this.programId,
      data,
    });
  }
}
