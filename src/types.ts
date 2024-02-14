export type ReplaceKeyConfig = {
  queryRelatedItems?: boolean; // Whether to query for related items
  tableName: string; // The name of the DynamoDB table to operate on
};

export class DatabaseOperationError extends Error {
  public cause?: Error;
  public operation?: string;
  public metadata?: Record<string, unknown>;

  constructor(
    message: string,
    {
      cause,
      operation,
      metadata,
    }: {
      cause?: Error;
      operation?: string;
      metadata?: Record<string, unknown>;
    } = {}
  ) {
    super(message);
    this.name = "DatabaseOperationError";
    this.cause = cause;
    this.operation = operation;
    this.metadata = metadata;

    // Maintaining proper stack trace for where our error was thrown (only supported in V8)
    // @ts-ignore
    if (Error.captureStackTrace) {
      // @ts-ignore
      Error.captureStackTrace(this, DatabaseOperationError);
    }
  }
}

// types.ts

export type DynamoDBKey = {
  partitionKey: string;
  sortKey?: string;
};
