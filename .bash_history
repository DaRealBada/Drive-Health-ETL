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
git add .
git commit -m "Initial commit"
git config --global user.name "DaRealBada"
git config --global user.email "tolubada1@gmail.com"
git commit -m "Initial commit"
git remote add origin https://github.com/DaRealBada/Drive-Health-ETL.git
git branch -M main
git push -u origin main
npm run start
npm install
npm run start
git add .
git commit -m "Add PII hashing for patient data"
git push
git pull origin main --allow-unrelated-histories
git push origin main
git config pull.rebase false  # merge
git pull origin main --allow-unrelated-histories
git merge --abort
rm -rf .git
mkdir Drive-Health-ETL
mv index.js package.json Drive-Health-ETL/
cd Drive-Health-ETL
echo "node_modules/" > .gitignore
git init
git add .
git commit -m "Initial commit of ETL service"
git branch -M main
git remote add origin https://github.com/DaRealBada/Drive-Health-ETL.git
git pull origin main --allow-unrelated-histories
git push -u origin main
git config --global pull.rebase false
git pull origin main --allow-unrelated-histories
git checkout --ours index.js
git checkout --ours package.json
git add .
git commit -m "Merge remote changes and resolve conflicts"
git push origin main
gcloud run deploy my-etl-service --source . --region us-central1
quit
exit
rm Dockerfile README.txt
mv package-lock.json node_modules/ Drive-Health-ETL/
cd Drive-Health-ETL
gcloud run deploy my-etl-service --source . --region us-central1
gcloud run deploy my-etl-service --source . --region us-central1
gcloud run deploy my-etl-service --source . --region us-central1
gcloud run deploy my-etl-service --source . --region us-central1
gcloud run deploy my-etl-service --source . --region us-central1
git add .
git commit -m "Refactor to single-message push model and add PII hashing"
git push
git push --set-upstream origin main
gcloud run deploy my-etl-service --source . --region us-central1
cd ~/Drive-Health-ETL
gcloud run deploy my-etl-service --source . --region us-central1
npm install
git add .
git commit -m "feat: Add structured logging with Winston"
git push
gcloud run deploy my-etl-service --source . --region us-central1
gcloud run deploy my-etl-service --source . --region us-central1
gcloud run deploy my-etl-service --source . --region us-central1
gcloud auth login
gcloud run deploy my-etl-service --source . --region us-central1
gcloud auth login
cd ~/Drive-Health-ETL
npm install libphonenumber-js
curl -X POST -H "Content-Type: application/json" -d '{
  "message": {
    "data": "ewogICJlbnZlbG9wZV92ZXJzaW9uIjogMSwKICAiZXZlbnRfdHlwZSI6ICJjYWxsLm1ldGFkYXRhIiwKICAic2NoZW1hX3ZlcnNpb24iOiAxLAogICJ0ZW5hbnRfaWQiOiAib3JnLWRlbW8iLAogICJ0cmFjZV9pZCI6ICJ0cmFjZS0wMDEiLAogICJvY2N1cnJlZF9hdCI6ICIyMDI1LTA4LTI4VDEyOjAwOjAwWiIsCiAgInNvdXJjZSI6ICJ0d2lsaW8iLAogICJwYXlsb2FkIjogewogICAgImNhbGxfaWQiOiAiY2FsbC0wMDEiLAogICAgImNhbGxlciI6ICIrMSAoNDE1KSA1NTUtMDAwMSIsCiAgICAiY2FsbGVlIjogIis0NCAyMCA3MTIzIDQ1NjciLAogICAgImR1cmF0aW9uX3NlY29uZHMiOiAxMjMsCiAgICAiZGlzcG9zaXRpb24iOiAiYW5zd2VyZWQiCiAgfQp9",
    "messageId": "1234567890"
  },
  "subscription": "projects/your-project/subscriptions/your-sub"
}' http://localhost:8080/pubsub
cd ~/Drive-Health-ETL
npm install --save-dev jest
npx jest
node index.js
npx jest
npx jest
npx jest
npx jest
cd ~/Drive-Health-ETL
npx jest
nano README.md
git add .
git commit -m "feat: Complete Milestone A with unit tests and README"
git push
cd ~/Drive-Health-ETL
gcloud pubsub subscriptions describe etl-processor-sub
mkdir Drive-Health-Replay-Job
cd Drive-Health-Replay-Job
nano package.json
nano index.js
bq mk   --table   --time_partitioning_field=occurred_at   --time_partitioning_type=DAY   --time_partitioning_expiration=365   --clustering_fields=tenant_id,event_type   --schema=tenant_id:STRING,event_type:STRING,schema_version:INTEGER,envelope_version:INTEGER,trace_id:STRING,occurred_at:TIMESTAMP,received_at:TIMESTAMP,source:STRING,sampled:BOOLEAN,idempotency_key:STRING,payload:JSON   drivehealth_dw.events
bq update   --table   --time_partitioning_expiration=31536000   drivehealth_dw.events
npm install
gcloud run jobs deploy drive-health-replay-job   --source .   --region us-central1
gcloud pubsub topics publish phone-call-metadata --message='{"envelope_version": 1, "event_type": "call.metadata", "schema_version": 1, "occurred_at": "2025-08-28T12:02:00Z", "payload": { "call_id": "call-bad-001" }}'
gcloud pubsub topics publish phone-call-metadata --message='{"envelope_version": 1, "event_type": "call.metadata", "schema_version": 1, "occurred_at": "2025-08-28T12:02:00Z", "payload": { "call_id": "call-bad-001" }}'
gcloud pubsub topics publish phone-call-metadata --message='{"envelope_version": 1, "event_type": "call.metadata", "schema_version": 1, "occurred_at": "2025-08-28T12:02:00Z", "payload": { "call_id": "call-bad-002" }}'
gcloud run services describe my-etl-service --region us-central1 --format 'value(status.url)'
gcloud pubsub topics publish phone-call-metadata --message='{"envelope_version": 1, "event_type": "call.metadata", "schema_version": 1, "occurred_at": "2025-08-28T12:02:00Z", "payload": { "call_id": "call-bad-002" }}'
gcloud run deploy my-etl-service --source . --region us-central1
gcloud pubsub topics publish phone-call-metadata --message='{"envelope_version": 1, "event_type": "call.metadata", "schema_version": 1, "occurred_at": "2025-08-28T12:02:00Z", "payload": { "call_id": "call-bad-002" }}'
gcloud pubsub topics publish phone-call-metadata --message='{"envelope_version": 1, "event_type": "call.metadata", "schema_version": 1, "occurred_at": "2025-09-02T12:00:00Z", "payload": { "call_id": "dlq-test-001" }}'
cd ~/Drive-Health-Replay-Job
gcloud run jobs deploy drive-health-replay-job   --source .   --region us-central1
cd ~/Drive-Health-Replay-Job
gcloud run jobs deploy drive-health-replay-job --source . --region us-central1
cd ~src
cd~ src
cd ~ src
cd ~Drive-Health-ETL
cd ~/src
cd ~/Drive-Health-ETL
git add .
git init
git remote add origin https://github.com/DaRealBada/Drive-Health-ETL.git
git add .
git commit -m "feat: Complete Milestone A with unit tests and README"
git branch -M main
git push --set-upstream origin main
git pull origin main --allow-unrelated-histories
git push origin main
git checkout --ours index.js
git checkout --ours package.json
git add .
git commit -m "Merge remote changes and resolve conflicts"
git push origin main
gcloud run services update drive-health-replay-job \
  --region=us-central1 \
  --update-env-vars=PARKING_LOT_TOPIC=phone-call-metadata-parking-lot
gcloud config set run/region us-central1
  --update-env-vars=PARKING_LOT_TOPIC=phone-call-metadata-parking-lot
gcloud run services update drive-health-replay-job \
  --region=us-central1 \
  --update-env-vars=PARKING_LOT_TOPIC=phone-call-metadata-parking-lot
gcloud run services update drive-health-replay-job --region=us-central1 --update-env-vars=PARKING_LOT_TOPIC=phone-call-metadata-parking-lot
gcloud run services list
gcloud run services update my-etl-service   --region=us-central1   --update-env-vars=PARKING_LOT_TOPIC=phone-call-metadata-parking-lot
gcloud pubsub topics publish phone-call-metadata   --message='{
    "envelope_version": 1,
    "event_type": "call.metadata",
    "schema_version": 1,
    "tenant_id": "org-demo",
    "trace_id": "trace-test-001",
    "occurred_at": "2025-09-02T20:20:00Z",
    "source": "manual-test",
    "payload": {
      "call_id": "call-test-001",
      "caller": "+1 (415) 555-0001",
      "callee": "+44 20 7123 4567",
      "duration_seconds": 123,
      "disposition": "answered"
    }
  }'
cd ~/Drive-Health-ETL
git add .
git init
git commit -m "feat: Complete Milestone D"
git branch -M main
git push --set-upstream origin main
git push origin main
# Stage all changes in the current directory and its subdirectories
git add .
# Create a new commit with a descriptive message
git commit -m "feat: Push all project files and folders"
# Push the committed changes to the 'main' branch of your remote repository
git push origin main
git add .
# Stage all changes in the current directory and its subdirectories
git add .
# Create a new commit with a descriptive message
git commit -m "feat: Push all project files and folders"
# Push the committed changes to the 'main' branch of your remote repository
git push origin main
git add docs
git add docs
cd ..
git init
git remote add origin https://github.com/DaRealBada/Drive-Health-ETL.git
cd ..
git add .
git commit -m "feat: Push top-level directories and files"
git add .
git commit -m "feat: Push top-level directories and files"
git push --force origin main
git pull origin main
git checkout main
git pull origin main --allow-unrelated-histories
git add .
git commit -m "feat: Push top-level directories and files"
git push origin main
git branch -M main
git push origin main
rm app.js
rm bq.js
rm handler.js
rm index.js
rm index.test.js
rm logger.js
rm package-lock.json
rm package.json
rm phone.js
rm README.md
rm sampling.js
rm validation.js
git add Drive-Health-ETL docs scripts
git commit -m "feat: Finalize project structure with all folders"
git commit -m "feat: Finalize project structure with all folders"
git push origin main
git remote add origin https://github.com/DaRealBada/Drive-Health-ETL.git
git branch -M main
git push -u origin main
git add Drive-Health-ETL docs scripts
git commit -m "feat: Finalize project structure with all folders"
git add -A
git commit -m "feat: Finalize project structure with folders and Dockerfile"
git push origin main
ls Drive-HealthETL
ls Drive-Health-ETL
mkdir Drive-Health-ETL
mv app.js bq.js handler.js index.js index.test.js logger.js package.json package-lock.json phone.js README.md sampling.js validation.js Dockerfile Drive-Health-ETL/
mkdir Drive-Health-ETL
mv app.js bq.js handler.js index.js index.test.js logger.js package.json package-lock.json phone.js README.md sampling.js validation.js Dockerfile Drive-Health-ETL/
mv Drive-Health-ETL src
git add src
git rm -r Drive-Health-ETL
git commit -m "refactor: rename Drive-Health-ETL folder to src"
git push origin main
ls -l | grep src
git rm src
git rm --cached src
rm -rf .git/modules/src
rm .gitmodules
git add src
git commit -m "fix: remove submodule and restore src as regular folder"
git push origin main
rm -rf src/.git
git config -f .git/config --remove-section submodule.src
git add src
git commit -m "fix: remove embedded repo and restore src as normal folder"
git push origin main
git add -A
git commit -m "chore: commit all modified and deleted files"
git push origin main
rm -rf src/.git
git init
git remote add origin https://github.com/DaRealBada/Drive-Health-ETL.git
git add .
git commit -m "feat: initial commit with src, docs, and scripts as directories"
git branch -M main
git push -u origin main
rm -rf src/.git
rm -rf docs/.git
rm -rf scripts/.git
rm -rf .git
git init
git remote add origin https://github.com/DaRealBada/Drive-Health-ETL.git
git remote add origin https://github.com/DaRealBada/Drive-Health-ETL.git
git add src docs scripts
git commit -m "feat: push only src, docs, and scripts folders"
git branch -M main
git push -u origin main
cd scripts
node 01_smoke_publish.js
cd ..
npm install
cd Drive-Health-ETL/src
cd ~/src
npm install
cd scripts
cd ~/scripts
node 01_smoke_publish.js
cd ~
npm install
node scripts/01_smoke_publish.js
node scripts/02_idempotency_test.js
node scripts/03_dlq_and_replay.js
node scripts/04_step_load_test.js
node scripts/01_smoke_publish.js
node scripts/04_step_load_test.js
node scripts/04_step_load_test.js
