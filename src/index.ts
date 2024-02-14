import express from "express";
import { json } from "body-parser";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { Repository } from "./respo";
import { Book, BookSchema } from "./service"; // Assuming you have a book.ts that exports Book and BookSchema

// Initialize DynamoDB Client
const dbClient = new DynamoDBClient({
  endpoint: "http://localhost:8000",
  region: "us-west-2",
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
// app.post("/books", async (req, res) => {
//   try {
//     const book = BookSchema.parse(req.body);
//     // Here, you would call a method from your repository to add the book to DynamoDB
//     // Assuming addBook is implemented in your repository
//     await bookRepository.addBook(book);
//     res.status(201).json(book);
//   } catch (error) {
//     res.status(400).json({ error: error.message });
//   }
// });

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
