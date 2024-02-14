import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  ScanCommand,
  QueryCommand,
  GetCommand,
  TransactWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";

import { DynamoDBKey, ReplaceKeyConfig, DatabaseOperationError } from "./types"; // Placeholder for actual type imports.

import express from "express";
import { json } from "body-parser";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { z } from "zod";

export const BookSchema = z.object({
  title: z.string(),
  author: z.string(),
  isbn: z.string(),
  publicationDate: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/, "Invalid date format, expected YYYY-MM-DD"),
  genre: z.string(),
  price: z.number().positive(),
});

export type Book = z.infer<typeof BookSchema>;

// Example usage
const myBook = BookSchema.parse({
  title: "The Great Gatsby",
  author: "F. Scott Fitzgerald",
  isbn: "9780743273565",
  publicationDate: "1925-04-10",
  genre: "Tragedy",
  price: 10.99,
});

export class AttributeRegistry {
  attributeI = 0;
  valueI = 0;

  namesMap: Map<string, string> = new Map();
  valuesMap: Map<any, string> = new Map();

  key(key: string) {
    if (!this.namesMap.has(key)) {
      const name = `#attr${this.attributeI}`;
      this.namesMap.set(key, name);
      this.attributeI += 1;
    }

    return this.namesMap.get(key);
  }

  value(value: any) {
    if (!this.valuesMap.has(value)) {
      const name = `:value${this.valueI}`;
      this.valuesMap.set(value, name);
      this.valueI += 1;
    }
    return this.valuesMap.get(value);
  }

  private mapToObject(thing: Map<any, string>) {
    const entries = thing.entries();
    const arr = Array.from(entries);
    const obj = Object.fromEntries(arr.map(([key, value]) => [value, key]));
    return obj;
  }

  get() {
    return {
      ExpressionAttributeNames: this.mapToObject(this.namesMap),
      ExpressionAttributeValues: this.mapToObject(this.valuesMap),
    };
  }
}

export function getDDBUpdateExpression<T>(item: T) {
  const registry = new AttributeRegistry();

  const UpdateExpression = `set ${Object.entries(item as any)
    .map(([key, value]) => {
      return `${registry.key(key)} = ${registry.value(value)}`;
    })
    .join(", ")}`;

  return {
    ...registry.get(),
    UpdateExpression,
  };
}

class Repository<T extends Record<string, any>> {
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

  // In your Repository class

  async getBookWithRelatedItems(
    partitionKey: string
  ): Promise<{ book: T | null; relatedItems: T[] }> {
    try {
      // Fetch the book item
      const book = await this.db.send(
        new GetCommand({
          TableName: this.tableName,
          Key: { partitionKey },
        })
      );

      // Fetch related items, assuming 'relatedPartitionKey' is the GSI for related items
      const relatedItemsResult = await this.db.send(
        new QueryCommand({
          TableName: this.tableName,
          IndexName: "RelatedItemsIndex", // The name of the GSI
          KeyConditionExpression: "relatedPartitionKey = :partitionKey",
          ExpressionAttributeValues: {
            ":partitionKey": partitionKey,
          },
        })
      );

      return {
        // @ts-ignore
        book: book.Item || null,
        relatedItems: relatedItemsResult.Items as T[],
      };
    } catch (error) {
      console.error("Failed to fetch book and related items:", error);
      throw new DatabaseOperationError(
        `Error fetching book and related items`,
        // @ts-ignore
        error
      );
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
    console.log("Preparing transaction items...");
    // Logic to add transaction items based on oldPrimaryKey and newPrimaryKey
    const transactionItems: any = []; // Example initialization, populate this based on your logic

    // Example logging to inspect the constructed transaction items
    console.log("Transaction Items:", transactionItems);
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

// Initialize DynamoDB Client
const dbClient = new DynamoDBClient({
  endpoint: "http://localhost:8000",
  region: "localhost",
  credentials: {
    accessKeyId: "fakeMyKeyId",
    secretAccessKey: "fakeSecretAccessKey",
  },
});

const docClient = DynamoDBDocumentClient.from(dbClient);
const bookRepository = new Repository<Book>("BooksTable", docClient);

const app = express();
const port = 3000;

app.use(json());

app.post("/books", async (req, res) => {
  try {
    const book = BookSchema.parse(req.body);

    // Prepare the Put operation for DynamoDB
    const putOperation = {
      Put: {
        TableName: "BooksTable", // Ensure this is accessible
        Item: book,
      },
    };

    // Execute the transaction with the mutate method
    await bookRepository.mutate([putOperation]);

    res.status(201).json(book);
  } catch (error) {
    // @ts-ignore
    res.status(400).json({ error: error.message });
  }
});

// Endpoint to update a book's primary key
app.patch("/books/:oldBookId", async (req, res) => {
  const { oldBookId } = req.params;
  const { newBookId } = req.body;
  if (!newBookId) {
    return res.status(400).json({ error: "New book ID is required." });
  }
  try {
    // Assuming your repository's replacePrimaryKey method is correctly implemented
    const result = await bookRepository.replacePrimaryKey(
      { partitionKey: oldBookId },
      { partitionKey: newBookId },
      req.body.otherAttributes,
      { queryRelatedItems: true, tableName: "BookReviewsTable" } // Example config
    );
    res.status(200).json(result);
  } catch (error) {
    // @ts-ignore
    res.status(500).json({ error: error.message });
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
