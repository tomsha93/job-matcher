import 'dotenv/config';
import express from 'express';

const app = express();
const PORT = process.env.PORT || 8080;

app.get('/', (req, res) => {
  res.send('Job Matcher Service is running!');
});

app.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});