async function writeBatchToBigQuery(events) {
  const startTime = Date.now();
  
  try {
    const rowsToInsert = events.map(({ envelope, processedPayload, idempotencyKey }) => 
      createBigQueryRow(envelope, processedPayload, idempotencyKey)
    );
    
    // DEBUG: Log what we're trying to insert
    console.log('=== BIGQUERY INSERT DEBUG ===');
    console.log('Rows to insert:', JSON.stringify(rowsToInsert, null, 2));
    console.log('============================');
    
    await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .insert(rowsToInsert);
    const processingTime = Date.now() - startTime;
    
    logger.info('BigQuery batch insert success', {
      batch_size: events.length,
      processing_time_ms: processingTime,
      insert_status: 'BATCH_SUCCESS'
    });
    
    return { success: true, errors: [] };
    
  } catch (error) {
    console.log('=== BIGQUERY ERROR DEBUG ===');
    console.log('Error name:', error.name);
    console.log('Error message:', error.message);
    
    if (error.errors && error.errors.length > 0) {
      console.log('Detailed errors:');
      error.errors.forEach((err, index) => {
        console.log(`Error ${index + 1}:`, JSON.stringify(err, null, 2));
      });
    }
    console.log('===========================');
    
    // Rest of your error handling...
    const processingTime = Date.now() - startTime;
    logger.error('BigQuery batch insert failed', {
      batch_size: events.length,
      error: error.message,
      processing_time_ms: processingTime,
      insert_status: 'BATCH_ERROR'
    });
    
    return { success: false, errors: error.errors || [error] };
  }
}
