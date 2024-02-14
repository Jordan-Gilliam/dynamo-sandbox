import express from "express";
import { json } from "body-parser";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { Repository } from "./respo";
import { Book, BookSchema } from "./service"; // Assuming you have a book.ts that exports Book and BookSchema

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

// Endpoint to add a new book
app.post("/books", async (req, res) => {
  try {
    // Validate the incoming book data against the Zod schema
    const validatedBook = BookSchema.parse(req.body);

    // Prepare the Put operation for the new book
    const putOperation = {
      Put: {
        TableName: "BookReviewsTable", // Assuming tableName is accessible; adjust as needed
        Item: validatedBook,
      },
    };

    // Call the mutate method with the prepared Put operation
    await bookRepository.mutate([putOperation]); // mutate expects an array of operations

    // Respond with the added book data
    res.status(201).json(validatedBook);
  } catch (error) {
    // Respond with an error message if validation or the database operation fails
    res.status(400).json({
      error: error instanceof Error ? error.message : "An error occurred",
    });
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
