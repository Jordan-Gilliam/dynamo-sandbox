import { z } from "zod";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
// Assuming `Repository`, `Book`, and `ReplaceKeyConfig` types are defined in your project
import { Repository } from "./respo"; // Update the import path according to where your Repository class is defined

// First, create an instance of DynamoDBClient
const dbClient = new DynamoDBClient({
  endpoint: "http://localhost:8000", // Local DynamoDB Endpoint
  region: "localhost", // Use a dummy region for local development
  credentials: {
    // Dummy credentials for local development
    accessKeyId: "fakeMyKeyId",
    secretAccessKey: "fakeSecretAccessKey",
  },
});

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

console.log(myBook);

// Then, use that instance to create DynamoDBDocumentClient
const docClient = DynamoDBDocumentClient.from(dbClient);

const tableName = "BooksTable"; // Your DynamoDB table name

// Ensure to pass `docClient` to the Repository, not `dbClient`
const bookRepository = new Repository<Book>(tableName, docClient);

export async function updateBookPrimaryKey(
  oldBookId: string,
  newBookId: string
) {
  const oldPrimaryKey = { partitionKey: oldBookId };
  const newPrimaryKey = { partitionKey: newBookId };
  const otherAttributes = {
    title: "Updated Book Title",
    author: "Updated Author Name",
  };

  const config = {
    queryRelatedItems: true, // Set to true if you want to query for related items, e.g., book reviews
    tableName: "BookReviewsTable", // Assuming reviews are stored in a separate table
  };

  try {
    const { newItem, relatedItems } = await bookRepository.replacePrimaryKey(
      oldPrimaryKey,
      newPrimaryKey,
      // @ts-ignore
      otherAttributes,
      config
    );

    console.log("Updated book item:", newItem);
    if (relatedItems) {
      console.log("Related book reviews that need updating:", relatedItems);
    }
  } catch (error) {
    console.error("Failed to update book primary key:", error);
  }
}

// Example usage
updateBookPrimaryKey("oldBookId123", "newBookId456");
