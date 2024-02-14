import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  ScanCommand,
  QueryCommand,
  TransactWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";
import { getDDBUpdateExpression } from "./util-expression";
import { DynamoDBKey, ReplaceKeyConfig, DatabaseOperationError } from "./types"; // Placeholder for actual type imports.

export class Repository<T extends Record<string, any>> {
  private tableName: string;
  private db: DynamoDBDocumentClient;

  constructor(tableName: string, dbClient: DynamoDBDocumentClient) {
    this.tableName = tableName;
    this.db = dbClient;
  }

  /**
   * Generic method to perform transactional mutations on DynamoDB.
   * @param operations Array of operations (Put, Update, Delete) to be performed in the transaction.
   * @returns Promise<void>
   */
  async mutate(
    operations: TransactWriteCommandInput["TransactItems"]
  ): Promise<void> {
    try {
      // Ensure there are operations to perform
      if (operations?.length === 0) {
        throw new Error("No operations provided for transaction.");
      }

      const commandInput: TransactWriteCommandInput = {
        TransactItems: operations?.map((op) => ({
          ...op,
          // Specify the table name for each operation if not already specified
          // This assumes operations provided don't already specify TableName
          Put: op.Put ? { ...op.Put, TableName: this.tableName } : undefined,
          Update: op.Update
            ? { ...op.Update, TableName: this.tableName }
            : undefined,
          Delete: op.Delete
            ? { ...op.Delete, TableName: this.tableName }
            : undefined,
        })),
      };

      await this.db.send(new TransactWriteCommand(commandInput));
    } catch (error) {
      this.handleDynamoDBError(error, "mutate", {});
      throw error; // Rethrow after logging/handling
    }
  }

  // Function to replace an item's primary key and optionally query for related items.
  async replacePrimaryKey(
    oldPrimaryKey: DynamoDBKey,
    newPrimaryKey: DynamoDBKey,
    otherAttributes: Omit<T, "partitionKey" | "sortKey">,
    config: ReplaceKeyConfig
  ): Promise<{ newItem: T; relatedItems?: T[] | null }> {
    const transactItems = await this.prepareCascadeUpdateParams(
      oldPrimaryKey,
      newPrimaryKey,
      config
    );

    // Begin a transaction to atomically replace the primary key and optionally update related items.
    try {
      await this.db.send(
        new TransactWriteCommand({ TransactItems: transactItems })
      );
    } catch (error) {
      this.handleDynamoDBError(error, "replacePrimaryKey", {
        oldPrimaryKey,
        newPrimaryKey,
      });
    }

    // Optionally query for related items if specified in the configuration.
    let relatedItems: T[] | null = null;
    if (config.queryRelatedItems) {
      relatedItems = await this.queryRelatedItems(
        oldPrimaryKey.partitionKey,
        true,
        config.tableName
      );
    }

    // Return the new item and any found related items.
    // const newItem = { ...otherAttributes, ...newPrimaryKey };

    const newItem: T = { ...otherAttributes, ...newPrimaryKey } as unknown as T;

    return { newItem, relatedItems };
  }

  private async prepareCascadeUpdateParams(
    oldPrimaryKey: DynamoDBKey,
    newPrimaryKey: DynamoDBKey,
    config: ReplaceKeyConfig
  ): Promise<any[]> {
    // Assume related items are identified by a 'parentId' attribute that matches the 'partitionKey' of the main item.
    const relatedItems = await this.queryRelatedItems(
      oldPrimaryKey.partitionKey,
      false
    ); // Using 'false' to indicate a full table scan.

    // Use getDDBUpdateExpression to prepare update expressions for each related item.
    return relatedItems.map((item) => {
      // Prepare the new values for the related item's 'parentId' attribute to link to the new primary key.
      const updateItem = {
        parentId: newPrimaryKey.partitionKey, // Adjust 'parentId' to your actual attribute name if different.
      };

      // Generate the update expression using getDDBUpdateExpression.
      const {
        UpdateExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
      } = getDDBUpdateExpression(updateItem);

      return {
        Update: {
          TableName: config.tableName, // Or the related items' table name if they reside in a different table.
          Key: {
            partitionKey: item.partitionKey,
            sortKey: item.sortKey ?? undefined,
          }, // Ensure to handle cases where sortKey might not exist.
          UpdateExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues,
        },
      };
    });
  }

  // Function to query related items based on the partition key, supporting both GSI queries and full table scans.
  async queryRelatedItems(
    partitionKey: string,
    useGSI: boolean = true,
    gsiName?: string
  ): Promise<T[]> {
    if (useGSI && gsiName) {
      return this.queryGSI(partitionKey, gsiName);
    } else {
      return this.scanTable(partitionKey);
    }
  }

  private async queryGSI(partitionKey: string, gsiName: string): Promise<T[]> {
    try {
      const params = {
        TableName: this.tableName,
        IndexName: gsiName,
        KeyConditionExpression: "#pk = :pkValue",
        ExpressionAttributeNames: {
          "#pk": "partitionKeyAttribute", // Replace 'partitionKeyAttribute' with the actual partition key attribute used in the GSI
        },
        ExpressionAttributeValues: {
          ":pkValue": partitionKey,
        },
      };

      const result = await this.db.send(new QueryCommand(params));
      return result.Items as T[];
    } catch (error) {
      console.error(
        `Error querying GSI (${gsiName}) with partition key (${partitionKey}):`,
        error
      );
      throw error;
    }
  }

  private async scanTable(partitionKey: string): Promise<T[]> {
    try {
      const params = {
        TableName: this.tableName,
        FilterExpression: "#pk = :pkValue",
        ExpressionAttributeNames: {
          "#pk": "partitionKey", // Adjust this to the actual partition key attribute name in your table
        },
        ExpressionAttributeValues: {
          ":pkValue": partitionKey,
        },
      };

      const result = await this.db.send(new ScanCommand(params));
      return result.Items as T[];
    } catch (error) {
      console.error(
        "Error scanning table for partition key:",
        partitionKey,
        error
      );
      throw error;
    }
  }

  // Adjusted handleDynamoDBError to include optional action and metadata parameters
  private handleDynamoDBError(
    error: any,
    action: string = "unknown action",
    metadata: Record<string, any> = {}
  ): void {
    console.error(`Error during ${action}:`, error, metadata);
    throw new DatabaseOperationError(`Error during ${action}`, error);
  }
}
