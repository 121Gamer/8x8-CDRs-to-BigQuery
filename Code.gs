/**
 * Google Apps Script for fetching 8x8 Call Detail Records (CDRs)
 * and loading them into a Google BigQuery table.
 *
 * This version is corrected based on a working code snippet provided by the user,
 * implementing scrollId pagination, using the correct pbxName for filtering,
 * and aligning the BigQuery field mapping.
 * v13: Implements a staging and merge pattern to prevent duplicate records.
 */

// --- Configuration ---
var CDR_API_ENDPOINT_BASE = 'https://api.8x8.com/analytics/work/v2/call-records';
var PBX_API_ENDPOINT = 'https://api.8x8.com/analytics/work/v2/customer-data/pbxes'; // This is confirmed to be working.
var TOKEN_ENDPOINT_V1 = 'https://api.8x8.com/analytics/work/v1/oauth/token';

var BIGQUERY_PROJECT_ID = '<YOUR>-project-8x8';
var BIGQUERY_DATASET_ID = '<YOUR>-8x8DataSet';
var BIGQUERY_TABLE_ID = '<YOUR>-8x8CallDetailRecords';
var BIGQUERY_STAGING_TABLE_ID = '<YOUR>-8x8CallDetailRecords_staging'; // Staging table for merging

var DAYS_TO_FETCH_FOR_SYNC = 1; // Number of past days to fetch data for.
var API_PAGE_SIZE = 500; // Page size from user's working code.
var API_TIMEZONE = 'Europe/Paris'; // Timezone required by the API, from user's working code.

var CACHE_ACCESS_TOKEN = '8x8_access_token_bq_sync_v1';
var userCache = CacheService.getUserCache();

// --- Main Orchestration Function ---
function sync8x8CallRecordsToBigQuery() {
  Logger.log('sync8x8CallRecordsToBigQuery CALLED - START');

  var scriptProperties = PropertiesService.getScriptProperties();
  var apiUsername = scriptProperties.getProperty('id');
  var apiPassword = scriptProperties.getProperty('pwd');
  var headerApiKey = scriptProperties.getProperty('apikey');

  if (!apiUsername || !apiPassword || !headerApiKey) {
    var missingProps = [];
    if (!apiUsername) missingProps.push("'id' (Username)");
    if (!apiPassword) missingProps.push("'pwd' (Password)");
    if (!headerApiKey) missingProps.push("'apikey' (Header API Key)");
    Logger.log('sync8x8CallRecordsToBigQuery: ERROR - Missing credentials from Script Properties: ' + missingProps.join(', '));
    throw new Error('Connector configuration error: Required 8x8 credentials not found in Script Properties: ' + missingProps.join(', '));
  }
  Logger.log('sync8x8CallRecordsToBigQuery - STEP 1: All Script Properties for auth found.');

  var tokenAuthData;
  try {
    tokenAuthData = get8x8AccessToken(apiUsername, apiPassword, headerApiKey);
  } catch (e) {
    Logger.log('sync8x8CallRecordsToBigQuery: ERROR - Failed to get 8x8 token data: ' + e.toString());
    throw new Error('Failed to authenticate with 8x8: ' + e.message);
  }
  Logger.log('sync8x8CallRecordsToBigQuery - STEP 2: Auth token data obtained successfully.');

  var endDate = new Date();
  var startDate = new Date();
  startDate.setDate(endDate.getDate() - DAYS_TO_FETCH_FOR_SYNC);
  
  var apiEndDateStr = Utilities.formatDate(endDate, "GMT", "yyyy-MM-dd HH:mm:ss");
  var apiStartDateStr = Utilities.formatDate(startDate, "GMT", "yyyy-MM-dd HH:mm:ss");

  Logger.log('sync8x8CallRecordsToBigQuery - STEP 3: Date range for fetching: %s to %s', apiStartDateStr, apiEndDateStr);

  try {
    var allCallRecords = [];
    Logger.log('sync8x8CallRecordsToBigQuery - STEP 4: Fetching PBX list...');
    var pbxes = fetchPbxList(tokenAuthData);
    if (!pbxes || pbxes.length === 0) {
        Logger.log('sync8x8CallRecordsToBigQuery: No PBXes found. Skipping CDR fetch.');
    } else {
        Logger.log('sync8x8CallRecordsToBigQuery: Found %s PBXes. Fetching CDRs for each.', pbxes.length);
        for (var i = 0; i < pbxes.length; i++) {
            var pbx = pbxes[i];
            Logger.log('Fetching CDRs for PBX Name: %s (ID: %s)', pbx.name, pbx.id);
            var pbxRecords = fetchAllCallRecordsForPbx(pbx.name, apiStartDateStr, apiEndDateStr, tokenAuthData);
            allCallRecords = allCallRecords.concat(pbxRecords);
            Utilities.sleep(500); // Be polite between fetching for different PBXes
        }
    }

    Logger.log('sync8x8CallRecordsToBigQuery - STEP 5: Total raw call records fetched across all PBXes: %s', allCallRecords.length);

    if (allCallRecords.length > 0) {
      var bqRows = allCallRecords.map(transformApiRecordToBigQueryRow).filter(function(row) { return row !== null; });
      Logger.log('sync8x8CallRecordsToBigQuery - STEP 6: Transformed %s records for BigQuery.', bqRows.length);

      if (bqRows.length > 0) {
        // MODIFICATION: The load function now handles the entire stage-and-merge process.
        loadAndMergeDataInBigQuery(bqRows);
        Logger.log('sync8x8CallRecordsToBigQuery - STEP 7: Staging and merging process initiated.');
      }
    }

    Logger.log('sync8x8CallRecordsToBigQuery FINISHED successfully.');

  } catch (e) {
    Logger.log('sync8x8CallRecordsToBigQuery: ERROR during data processing or BigQuery load: %s \n Stack: %s', e.toString(), e.stack);
    throw new Error('Error during 8x8 data sync: ' + e.message);
  }
}

// --- 8x8 Authentication ---
function get8x8AccessToken(apiUsername, apiPassword, headerApiKey, forceRefresh) {
  Logger.log('get8x8AccessToken CALLED - START. forceRefresh: %s', forceRefresh);
  forceRefresh = forceRefresh || false;
  var cachedToken = userCache.get(CACHE_ACCESS_TOKEN);

  if (cachedToken && !forceRefresh) {
    Logger.log('get8x8AccessToken: Using cached 8x8 access token.');
    return { accessToken: cachedToken, headerApiKey: headerApiKey };
  }

  if (!apiUsername || !apiPassword || !headerApiKey) {
    Logger.log('get8x8AccessToken: ERROR - Missing one or more credentials for token fetch.');
    throw new Error('Internal error: Missing credentials (Username, Password, Header API Key) for 8x8 token fetch.');
  }

  Logger.log('get8x8AccessToken: Fetching new 8x8 access token...');
  var tokenPayload = { 'username': apiUsername, 'password': apiPassword };
  var tokenHeaders = { '8x8-apikey': headerApiKey, 'Content-Type': 'application/x-www-form-urlencoded' };
  var options = {
    'method': 'post',
    'headers': tokenHeaders,
    'payload': Object.keys(tokenPayload).map(function(key) { return encodeURIComponent(key) + '=' + encodeURIComponent(tokenPayload[key]); }).join('&'),
    'muteHttpExceptions': true
  };

  var response = UrlFetchApp.fetch(TOKEN_ENDPOINT_V1, options);
  var responseCode = response.getResponseCode();
  var responseBody = response.getContentText();
  Logger.log('get8x8AccessToken: Token API Response Code: %s', responseCode);

  if (responseCode === 200) {
    var parsedData = JSON.parse(responseBody);
    var accessToken = parsedData.access_token;
    if (!accessToken) { throw new Error('Failed to retrieve access token from 8x8.'); }
    var expiresIn = parsedData.expires_in || 3000;
    userCache.put(CACHE_ACCESS_TOKEN, accessToken, Math.max(1, expiresIn - 60));
    Logger.log('get8x8AccessToken: Successfully fetched and cached new 8x8 access token.');
    return { accessToken: accessToken, headerApiKey: headerApiKey };
  } else {
    Logger.log('get8x8AccessToken: ERROR fetching token: ' + responseBody);
    throw new Error('Failed to retrieve access token from 8x8. Status: ' + responseCode);
  }
}

// --- 8x8 Data Fetching ---
function fetchPbxList(tokenAuthData) {
  // This function is confirmed to be working correctly. No changes needed.
  Logger.log('fetchPbxList (using _embedded.pbxList) CALLED - START.');
  var allPbxes = [];
  var nextPageUrl = PBX_API_ENDPOINT;

  var options = {
    'method': 'get',
    'headers': {
      'Authorization': 'Bearer ' + tokenAuthData.accessToken,
      '8x8-apikey': tokenAuthData.headerApiKey,
      'Accept': 'application/json'
    },
    'muteHttpExceptions': true
  };

  var pageCount = 0;
  var MAX_PBX_PAGES_TO_FETCH = 10;

  while (nextPageUrl && pageCount < MAX_PBX_PAGES_TO_FETCH) {
    pageCount++;
    var response = UrlFetchApp.fetch(nextPageUrl, options);
    var responseCode = response.getResponseCode();
    if (responseCode === 200) {
      var jsonResponse = JSON.parse(response.getContentText());
      var pbxListOnPage = (jsonResponse._embedded && Array.isArray(jsonResponse._embedded.pbxList)) ? jsonResponse._embedded.pbxList : [];
      Logger.log('fetchPbxList: Received %s PBX items on API call #%s.', pbxListOnPage.length, pageCount);
      pbxListOnPage.forEach(function(pbx) { if (pbx.id && pbx.name) { allPbxes.push({ id: pbx.id, name: pbx.name }); } });

      if (jsonResponse._links && jsonResponse._links.next && jsonResponse._links.next.href) {
        nextPageUrl = jsonResponse._links.next.href;
      } else {
        nextPageUrl = null;
      }
    } else {
      throw new Error('API Error fetching PBX list: ' + responseCode + ' - ' + response.getContentText());
    }
  }
  Logger.log('fetchPbxList: Total PBXes collected: %s.', allPbxes.length);
  return allPbxes;
}

function fetchAllCallRecordsForPbx(pbxName, startTime, endTime, tokenAuthData) {
    Logger.log('fetchAllCallRecordsForPbx CALLED for PBX Name: %s, Range: %s to %s', pbxName, startTime, endTime);
    var allRecordsForPbx = [];
    var scrollId = 0;

    var baseUrl = CDR_API_ENDPOINT_BASE
        + '?pbxId=' + encodeURIComponent(pbxName)
        + '&startTime=' + encodeURIComponent(startTime)
        + '&endTime=' + encodeURIComponent(endTime)
        + '&timeZone=' + encodeURIComponent(API_TIMEZONE)
        + '&pageSize=' + API_PAGE_SIZE;
        
    var options = {
        'method': 'get',
        'headers': {
            'Authorization': 'Bearer ' + tokenAuthData.accessToken,
            '8x8-apikey': tokenAuthData.headerApiKey
        },
        'muteHttpExceptions': true
    };
    
    var MAX_SCROLLS = 100; // Safety break to prevent infinite loops
    for (var j = 0; j < MAX_SCROLLS; j++) {
        var currentUrl;
        if (scrollId === 0 || scrollId === "No Data" || !scrollId) {
            currentUrl = baseUrl;
        } else {
            currentUrl = baseUrl + '&scrollId=' + encodeURIComponent(scrollId);
        }
        
        Logger.log("Fetching page %s for PBX %s. URL: %s", j + 1, pbxName, currentUrl);
        var response = UrlFetchApp.fetch(currentUrl, options);
        var responseCode = response.getResponseCode();
        var responseBody = response.getContentText();

        if (responseCode !== 200) {
            throw new Error('API Error fetching CDRs for PBX ' + pbxName + ': ' + responseCode + ' - ' + responseBody);
        }

        var dataCdr = JSON.parse(responseBody);
        var recordsOnPage = dataCdr.data;
        if (recordsOnPage && Array.isArray(recordsOnPage) && recordsOnPage.length > 0) {
            allRecordsForPbx = allRecordsForPbx.concat(recordsOnPage);
            Logger.log("Fetched %s records. Total for this PBX so far: %s", recordsOnPage.length, allRecordsForPbx.length);
        }

        scrollId = dataCdr.meta.scrollId;
        if (scrollId === "No Data" || !scrollId) {
            Logger.log("End of records for PBX %s. Final scrollId: %s", pbxName, scrollId);
            break; // Exit the loop
        }
    }
    
    return allRecordsForPbx;
}


// --- Data Transformation ---
function transformApiRecordToBigQueryRow(apiRecord) {
  try {
    var row = JSON.parse(JSON.stringify(apiRecord)); 
    
    var formatTimestamp = function(timestampMs) {
      if (!timestampMs || typeof timestampMs !== 'number' || timestampMs <= 0) {
        return null;
      }
      try {
        return new Date(timestampMs).toISOString();
      } catch (e) {
        return null;
      }
    };

    row.startTimeUTC = formatTimestamp(row.startTimeUTC);
    row.connectTimeUTC = formatTimestamp(row.connectTimeUTC);
    row.disconnectedTimeUTC = formatTimestamp(row.disconnectedTimeUTC);
    
    if (Array.isArray(row.departments)) {
      row.departments = row.departments.join(',');
    }
    if (Array.isArray(row.branches)) {
      row.branches = row.branches.join(',');
    }
    
    return row;

  } catch (e) {
    Logger.log('CRITICAL ERROR in transformApiRecordToBigQueryRow for record: ' + JSON.stringify(apiRecord) + '. Error: ' + e.toString());
    return null;
  }
}


// --- BigQuery Loading ---
/**
 * CORRECTED (v16): Uses a predefined schema for loading into the staging table to prevent type mismatches.
 */
function loadAndMergeDataInBigQuery(rows) {
  if (!rows || rows.length === 0) {
    Logger.log('loadAndMergeDataInBigQuery: No rows to load.');
    return;
  }
  
  Logger.log('loadAndMergeDataInBigQuery: Preparing to load %s rows into STAGING table: %s',
    rows.length, BIGQUERY_STAGING_TABLE_ID);

  var dataAsJson = rows.map(function(row) { return JSON.stringify(row); }).join('\n');
  var dataAsBlob = Utilities.newBlob(dataAsJson, 'application/json');
  
  // NEW: Define the schema explicitly to match the main table. This prevents auto-detect errors.
  var bqSchema = {
    fields: [
      {name: 'callId', type: 'STRING'},
      {name: 'sipCallId', type: 'STRING'},
      {name: 'pbxId', type: 'STRING'},
      {name: 'startTimeUTC', type: 'TIMESTAMP'},
      {name: 'connectTimeUTC', type: 'TIMESTAMP'},
      {name: 'disconnectedTimeUTC', type: 'TIMESTAMP'},
      {name: 'talkTime', type: 'STRING'},
      {name: 'talkTimeMS', type: 'INTEGER'},
      {name: 'callTime', type: 'INTEGER'},
      {name: 'ringDuration', type: 'INTEGER'},
      {name: 'waitTime', type: 'STRING'},
      {name: 'waitTimeMS', type: 'INTEGER'},
      {name: 'calleeHoldDuration', type: 'STRING'},
      {name: 'calleeHoldDurationMS', type: 'INTEGER'},
      {name: 'caller', type: 'STRING'},
      {name: 'callerId', type: 'STRING'},
      {name: 'callerName', type: 'STRING'},
      {name: 'callee', type: 'STRING'},
      {name: 'calleeName', type: 'STRING'},
      {name: 'dnis', type: 'STRING'},
      {name: 'direction', type: 'STRING'},
      {name: 'missed', type: 'STRING'},
      {name: 'abandoned', type: 'STRING'},
      {name: 'answered', type: 'STRING'},
      {name: 'lastLegDisposition', type: 'STRING'},
      {name: 'callLegCount', type: 'STRING'},
      {name: 'callerDisconnectOnHold', type: 'STRING'},
      {name: 'calleeDisconnectOnHold', type: 'STRING'},
      {name: 'departments', type: 'STRING'},
      {name: 'branches', type: 'STRING'},
      {name: 'startTime', type: 'STRING'},
      {name: 'connectTime', type: 'STRING'},
      {name: 'disconnectedTime', type: 'STRING'},
      {name: 'answeredTime', type: 'INTEGER'},
      {name: 'abandonedTime', type: 'INTEGER'},
      {name: 'aaDestination', type: 'STRING'}
    ]
  };

  var stagingJob = {
    configuration: {
      load: {
        destinationTable: {
          projectId: BIGQUERY_PROJECT_ID,
          datasetId: BIGQUERY_DATASET_ID,
          tableId: BIGQUERY_STAGING_TABLE_ID
        },
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        writeDisposition: 'WRITE_TRUNCATE',
        schema: bqSchema, // Use the predefined schema instead of auto-detect
        autodetect: false
      }
    }
  };

  try {
    var loadJob = BigQuery.Jobs.insert(stagingJob, BIGQUERY_PROJECT_ID, dataAsBlob);
    Logger.log('Load to staging table started. Job ID: %s', loadJob.jobReference.jobId);
    waitForJob(loadJob.jobReference.jobId);
  } catch (e) {
    Logger.log('Error inserting BigQuery STAGING load job: ' + e.toString());
    throw new Error('BigQuery staging load job insertion failed: ' + e.message);
  }
  
  Logger.log('loadAndMergeDataInBigQuery: Staging table loaded. Now running MERGE query.');
  runBigQueryMerge();
}

function waitForJob(jobId) {
  var sleepTimeMs = 500;
  while (true) {
    var job = BigQuery.Jobs.get(BIGQUERY_PROJECT_ID, jobId);
    if (job.status.state === 'DONE') {
      if (job.status.errorResult) {
        throw new Error('BQ job failed: ' + JSON.stringify(job.status.errors));
      }
      Logger.log('Job %s completed successfully.', jobId);
      return;
    }
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
  }
}

/**
 * Cleaned up (v16): Runs the MERGE query. No casting is needed now that the staging schema is defined.
 */
function runBigQueryMerge() {
  var mergeSql = `
    MERGE \`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_TABLE_ID}\` T
    USING \`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_STAGING_TABLE_ID}\` S
    ON T.callId = S.callId
    WHEN NOT MATCHED THEN
      INSERT ROW
    WHEN MATCHED THEN
      UPDATE SET
        T.sipCallId = S.sipCallId,
        T.pbxId = S.pbxId,
        T.startTimeUTC = S.startTimeUTC,
        T.connectTimeUTC = S.connectTimeUTC,
        T.disconnectedTimeUTC = S.disconnectedTimeUTC,
        T.talkTime = S.talkTime,
        T.talkTimeMS = S.talkTimeMS,
        T.callTime = S.callTime,
        T.ringDuration = S.ringDuration,
        T.waitTime = S.waitTime,
        T.waitTimeMS = S.waitTimeMS,
        T.calleeHoldDuration = S.calleeHoldDuration,
        T.calleeHoldDurationMS = S.calleeHoldDurationMS,
        T.caller = S.caller,
        T.callerId = S.callerId,
        T.callerName = S.callerName,
        T.callee = S.callee,
        T.calleeName = S.calleeName,
        T.dnis = S.dnis,
        T.direction = S.direction,
        T.missed = S.missed,
        T.abandoned = S.abandoned,
        T.answered = S.answered,
        T.lastLegDisposition = S.lastLegDisposition,
        T.callLegCount = S.callLegCount,
        T.callerDisconnectOnHold = S.callerDisconnectOnHold,
        T.calleeDisconnectOnHold = S.calleeDisconnectOnHold,
        T.departments = S.departments,
        T.branches = S.branches,
        T.startTime = S.startTime,
        T.connectTime = S.connectTime,
        T.disconnectedTime = S.disconnectedTime,
        T.answeredTime = S.answeredTime,
        T.abandonedTime = S.abandonedTime,
        T.aaDestination = S.aaDestination
  `;

  var mergeJob = {
    configuration: {
      query: {
        query: mergeSql,
        useLegacySql: false
      }
    }
  };

  try {
    var queryJob = BigQuery.Jobs.insert(mergeJob, BIGQUERY_PROJECT_ID);
    Logger.log('MERGE job started. Job ID: %s', queryJob.jobReference.jobId);
    waitForJob(queryJob.jobReference.jobId);
  } catch (e) {
    Logger.log('Error running MERGE job: ' + e.toString());
    throw new Error('BigQuery MERGE job failed: ' + e.message);
  }
}


// --- Utility / Test Functions ---
function manuallyFetchAndLogSampleCDRs() {
  Logger.log('manuallyFetchAndLogSampleCDRs CALLED');
  var scriptProperties = PropertiesService.getScriptProperties();
  var apiUsername = scriptProperties.getProperty('id');
  var apiPassword = scriptProperties.getProperty('pwd');
  var headerApiKey = scriptProperties.getProperty('apikey');
   if (!apiUsername || !apiPassword || !headerApiKey) {
    Logger.log('Missing credentials in Script Properties (id, pwd, apikey).');
    return;
  }

  var tokenAuthData;
  try {
    tokenAuthData = get8x8AccessToken(apiUsername, apiPassword, headerApiKey);
  } catch (e) {
    Logger.log('Failed to get token: ' + e.toString());
    return;
  }

  var endDate = new Date();
  var startDate = new Date();
  startDate.setDate(endDate.getDate() - 1);
  var apiEndDateStr = Utilities.formatDate(endDate, "GMT", "yyyy-MM-dd HH:mm:ss");
  var apiStartDateStr = Utilities.formatDate(startDate, "GMT", "yyyy-MM-dd HH:mm:ss");

  try {
    var pbxes = fetchPbxList(tokenAuthData);
    if (pbxes && pbxes.length > 0) {
      Logger.log('Fetching CDRs for first PBX: %s (%s)', pbxes[0].id, pbxes[0].name);
      var records = fetchAllCallRecordsForPbx(pbxes[0].name, apiStartDateStr, apiEndDateStr, tokenAuthData);
      if (records && records.length > 0) {
        Logger.log('Fetched %s records for PBX %s. Sample of first record:', records.length, pbxes[0].name);
        Logger.log(JSON.stringify(records[0], null, 2));
        
        Logger.log('Sample of transformed first record for BigQuery:');
        var transformedRecord = transformApiRecordToBigQueryRow(records[0]);
        Logger.log(JSON.stringify(transformedRecord, null, 2));
      } else {
        Logger.log('No records found for PBX %s in the date range.', pbxes[0].name);
      }
    } else {
        Logger.log('No PBXes found to test CDR fetching.');
    }
  } catch (e) {
    Logger.log('Error during manual fetch: ' + e.toString());
  }
}

/**
 * MODIFIED (v17): Handles GET requests to the web app URL and returns the full execution log.
 * This function acts as the trigger when the URL is visited.
 */
function doGet(e) {
  var startTime = new Date().toLocaleTimeString();
  var output = '8x8 to BigQuery Sync Triggered at: ' + startTime + '\n\n';
  
  try {
    sync8x8CallRecordsToBigQuery();
    
    // If successful, create a success message.
    output += '====================\n';
    output += '  SUCCESS \n';
    output += '====================\n\n';
    
  } catch (error) {
    // If an error occurs, create an error message.
    output += '====================\n';
    output += '  ERROR \n';
    output += '====================\n\n';
    output += 'Error Message: ' + error.message + '\n';
    output += 'Error Stack: ' + error.stack + '\n\n';
  }
  
  // Append the full log to the output.
  output += '--------------------\n';
  output += '  Execution Log \n';
  output += '--------------------\n';
  output += Logger.getLog();
  
  // Return the full log as pre-formatted text in an HTML page.
  return HtmlService.createHtmlOutput('<pre>' + output + '</pre>')
    .setTitle('8x8 Sync Execution Log');
}
