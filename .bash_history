gcloud compute ssh <INSTANCE_NAME> --zone=<ZONE>
gcloud compute ssh <team18-j> --zone=<ZONE>
ls la
ls -la
mv "Dockerfile.rtf" "Dockerfile"
mv "node.js" "index.js"
cat package.json
# Create clean package.json
cat > package.json << 'EOF'
{
  "name": "simple-etl",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": {
    "start": "node index.js"
  },
  "dependencies": {
    "express": "^4.18.2"
  }
}
EOF

# Create clean index.js
cat > index.js << 'EOF'
const express = require('express');
const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());

app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

app.post('/process-call', (req, res) => {
  console.log('Received message:', req.body);
  res.status(200).send('Message received');
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
EOF

# Create clean Dockerfile
cat > Dockerfile << 'EOF'
FROM node:18-slim
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
EXPOSE 8080
CMD ["npm", "start"]
EOF

ls -la
gcloud run deploy my-etl-service --source . --region us-central1 --allow-unauthenticated
cat > index.js << 'EOF'
const express = require('express');
const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());

app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

app.post('/process-call', (req, res) => {
  try {
    console.log('Received Pub/Sub message');
    
    // Decode the base64 message data
    const message = req.body.message;
    const callData = JSON.parse(Buffer.from(message.data, 'base64').toString());
    
    console.log('Decoded call data:', callData);
    
    // Random sampling - 5% selection
    if (Math.random() < 0.05) {
      console.log(`Call ${callData.call_id} SELECTED for audit`);
      // TODO: Send to BigQuery here
    } else {
      console.log(`Call ${callData.call_id} not selected for audit`);
    }
    
    res.status(200).send('Message processed');
  } catch (error) {
    console.error('Error processing message:', error);
    res.status(500).send('Error processing message');
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
EOF

gcloud run deploy my-etl-service --source . --region us-central1 --allow-unauthenticated
nano package.json
gcloud run deploy my-etl-service --source . --region us-central1
gcloud run deploy my-etl-service --source . --region us-central1 --allow-unauthenticated
git init
