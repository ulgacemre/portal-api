import { JWT } from 'google-auth-library';
import axios from 'axios';
import { Pool } from 'pg';
import dotenv from 'dotenv';
import prisma from './db.service';
import { formatRelativeTime, formatDateWithRelative } from './utils';

dotenv.config();

// --- Configuration ---
const SERVICE_ACCOUNT_JSON_STRING = process.env.GOOGLE_SERVICE_ACCOUNT_CREDENTIALS;
const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID;
const SHEET_NAME = 'Projects'; // Combined sheet for Projects and their Discord data
const PRIMARY_KEY_COLUMN_LETTER = 'A'; // Column letter for Primary Key in Sheets
const PRIMARY_KEY_COLUMN_INDEX = 0; // 0-based index for the primary key column (A=0, B=1, etc.)
const GOOGLE_TOKEN_URI = 'https://oauth2.googleapis.com/token';
const GOOGLE_SHEETS_API_ENDPOINT = 'https://sheets.googleapis.com/v4/spreadsheets';
const GOOGLE_SHEETS_SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets'
];

// Define expected column structure as a constant
// lastActivity is a timestamp showing when the project or its Discord data was last updated
const EXPECTED_COLUMN_STRUCTURE = [
  "id", "level", "projectName", "Test", "projectDescription", "projectVision", 
  "projectLinks", "referralSource", "scientificReferences", "credentialLinks", 
  "teamMembers", "motivation", "progress", 
  "id (Discord)", "serverId", "serverName", "invitationUrl",
  "messagesCount", "papersShared", "qualityScore", "memberCount", "lastActivity"
];

// Cache for validated column indices
let validatedColumnIndices: Record<string, number> | null = null;

// Force revalidation flag - set to true to revalidate the column structure on next operation
let forceRevalidation = false;

// Initialize PostgreSQL connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

/**
 * Get Google OAuth2 access token using service account credentials
 */
export async function getGoogleAccessToken(): Promise<string> {
  if (!SERVICE_ACCOUNT_JSON_STRING) {
    throw new Error('Missing GOOGLE_SERVICE_ACCOUNT_CREDENTIALS environment variable');
  }
  
  try {
    // Try to parse the service account credentials JSON
    let creds;
    try {
      creds = JSON.parse(SERVICE_ACCOUNT_JSON_STRING);
    } catch (jsonError) {
      // Provide detailed error about the JSON parsing issue
      const errorMsg = jsonError instanceof Error ? jsonError.message : String(jsonError);
      console.error('Failed to parse Google service account credentials JSON:', errorMsg);
      
      // Show a snippet of the credentials string to help diagnose (first 20 chars)
      const credSnippet = SERVICE_ACCOUNT_JSON_STRING.length > 20
        ? SERVICE_ACCOUNT_JSON_STRING.substring(0, 20) + '...'
        : SERVICE_ACCOUNT_JSON_STRING;
      console.error('Service account credentials snippet:', credSnippet);
      
      throw new Error(`Invalid Google service account credentials JSON: ${errorMsg}. Check that your GOOGLE_SERVICE_ACCOUNT_CREDENTIALS environment variable contains valid JSON.`);
    }
    
    // Check if the parsed credentials have the required fields
    if (!creds.client_email || !creds.private_key) {
      throw new Error('Google service account credentials are missing required fields (client_email and/or private_key)');
    }
    
    const client = new JWT({
      email: creds.client_email,
      key: creds.private_key,
      scopes: GOOGLE_SHEETS_SCOPES,
    });

    const token = await client.authorize();
    return token.access_token || '';
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error('Error getting Google access token:', errorMsg);
    throw new Error(`Failed to get Google access token: ${errorMsg}`);
  }
}

/**
 * Find the row index in Google Sheets that contains the given ID
 */
async function findRowIndexById(accessToken: string, id: string, sheetName: string = SHEET_NAME): Promise<number | null> {
  const range = `${sheetName}!${PRIMARY_KEY_COLUMN_LETTER}:${PRIMARY_KEY_COLUMN_LETTER}`;
  const url = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${range}?majorDimension=COLUMNS`;
  
  console.log(`[SHEETS_SEARCH] Searching for ID ${id} in Sheet "${sheetName}" range ${range}`);
  
  try {
    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`
      }
    });

    // The API returns values as an array of columns if majorDimension=COLUMNS.
    // We are interested in the first (and only requested) column.
    const idColumnData = response.data.values?.[0];
    if (!idColumnData || idColumnData.length === 0) {
      console.log(`[SHEETS_SEARCH] Primary key column (${PRIMARY_KEY_COLUMN_LETTER}) is empty or not found in sheet "${sheetName}".`);
      
      // Let's check if the sheet exists by fetching sheet metadata
      try {
        const sheetInfoUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}?fields=sheets.properties`;
        const sheetInfoResponse = await axios.get(sheetInfoUrl, {
          headers: {
            Authorization: `Bearer ${accessToken}`
          }
        });
        
        const sheets = sheetInfoResponse.data.sheets || [];
        const sheetExists = sheets.some((sheet: any) => sheet.properties?.title === sheetName);
        
        if (!sheetExists) {
          console.error(`[SHEETS_SEARCH] Sheet "${sheetName}" does not exist in the spreadsheet. Available sheets:`);
          sheets.forEach((sheet: any) => {
            console.log(`  - ${sheet.properties?.title}`);
          });
          return null;
        } else {
          console.log(`[SHEETS_SEARCH] Sheet "${sheetName}" exists but the primary key column is empty.`);
        }
      } catch (sheetInfoError) {
        console.error(`[SHEETS_SEARCH] Failed to get sheet metadata:`, sheetInfoError);
      }
      
      return null;
    }

    console.log(`[SHEETS_SEARCH] Found ${idColumnData.length} rows in primary key column`);
    
    // Log a few IDs from the column for debugging
    console.log(`[SHEETS_SEARCH] First few IDs in column (up to 5):`);
    idColumnData.slice(0, 5).forEach((value: any, index: number) => {
      console.log(`  Row ${index + 1}: ${value}`);
    });

    // Find the index of the cell that matches the id.
    // Ensure consistent string comparison as Sheet values might be numbers or strings.
    const rowIndex = idColumnData.findIndex((cellValue: any) => String(cellValue).trim() === String(id).trim());
    
    if (rowIndex === -1) {
      console.log(`[SHEETS_SEARCH] ID "${id}" not found in column ${PRIMARY_KEY_COLUMN_LETTER} of sheet "${sheetName}".`);
      
      // Try finding the ID in other columns of the sheet (e.g., Discord ID column)
      console.log(`[SHEETS_SEARCH] Attempting alternate search strategies for ID ${id}...`);
      
      // Check Discord ID column (Column M)
      const discordIdRange = `${sheetName}!M:M`;
      const discordIdUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${discordIdRange}?majorDimension=COLUMNS`;
      
      try {
        const discordIdResponse = await axios.get(discordIdUrl, {
          headers: {
            Authorization: `Bearer ${accessToken}`
          }
        });
        
        const discordIdColumnData = discordIdResponse.data.values?.[0];
        if (discordIdColumnData && discordIdColumnData.length > 0) {
          console.log(`[SHEETS_SEARCH] Checking Discord ID column for possible match to ${id}...`);
          
          // Find row with matching Discord ID
          const discordRowIndex = discordIdColumnData.findIndex((cellValue: any) => 
            cellValue && String(cellValue).trim() === String(id).trim()
          );
          
          if (discordRowIndex !== -1) {
            // Now fetch the project ID from this row to confirm it's the right one
            const projectIdRange = `${sheetName}!A${discordRowIndex + 1}:A${discordRowIndex + 1}`;
            const projectIdResponse = await axios.get(
              `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${projectIdRange}`,
              { headers: { Authorization: `Bearer ${accessToken}` } }
            );
            
            const projectId = projectIdResponse.data.values?.[0]?.[0];
            console.log(`[SHEETS_SEARCH] Found potential match in Discord ID column at row ${discordRowIndex + 1}. Project ID: ${projectId}`);
            
            return discordRowIndex + 1; // Return the row index (1-indexed)
          }
        }
      } catch (discordIdError) {
        console.error(`[SHEETS_SEARCH] Error searching Discord ID column:`, discordIdError);
      }
      
      // Fallback: try to find by projectId using database lookup
      try {
        console.log(`[SHEETS_SEARCH] Attempting database lookup for ID ${id}...`);
        // Check if this ID is either a project ID or a Discord ID
        
        // Try looking up as project ID first
        const projectResult = await pool.query(
          'SELECT id FROM "Project" WHERE id = $1',
          [id]
        );
        
        // If no project found, try looking up as Discord ID
        if (projectResult.rows.length === 0) {
          console.log(`[SHEETS_SEARCH] No project found with ID ${id}. Checking if it's a Discord ID...`);
          
          const discordResult = await pool.query(
            'SELECT "projectId" FROM "Discord" WHERE id = $1 OR "serverId" = $1',
            [id]
          );
          
          if (discordResult.rows.length > 0 && discordResult.rows[0].projectId) {
            const projectId = discordResult.rows[0].projectId;
            console.log(`[SHEETS_SEARCH] Found project ID ${projectId} for Discord ID/serverID ${id}`);
            
            // Try looking up project ID in the sheet
            const projectSearch = await findRowIndexById(accessToken, projectId);
            if (projectSearch !== null) {
              console.log(`[SHEETS_SEARCH] Found row with project ID ${projectId} at row ${projectSearch}`);
              return projectSearch;
            }
          }
        }
      } catch (dbError) {
        console.error(`[SHEETS_SEARCH] Error during database lookup:`, dbError);
      }
      
      return null;
    }

    // Google Sheets rows are 1-indexed.
    console.log(`[SHEETS_SEARCH] Found ID "${id}" at row index ${rowIndex + 1}`);
    return rowIndex + 1;
  } catch (error) {
    console.error(`[SHEETS_SEARCH] Error finding row for ID ${id} in sheet "${sheetName}":`, error);
    if (axios.isAxiosError(error) && error.response) {
      console.error(`[SHEETS_SEARCH] API response status: ${error.response.status}`);
      console.error(`[SHEETS_SEARCH] API response data:`, error.response.data);
    }
    return null;
  }
}

/**
 * Format a project record into a row for Google Sheets, including its Discord data if available
 * IMPORTANT: The order of values must match your Google Sheet columns
 */
function formatProjectToRow(record: any, discordRecord: any = null): any[] {
  if (!record) {
    console.warn("[SHEETS_SYNC] formatProjectToRow received null or undefined record. Returning empty array.");
    return [];
  }

  console.log('[SHEETS_SYNC] Formatting project data for row. Project ID:', record.id);

  // Using the globally defined EXPECTED_COLUMN_STRUCTURE for consistency
  console.log('[SHEETS_SYNC] Column structure being used:');
  EXPECTED_COLUMN_STRUCTURE.forEach((colName, idx) => {
    console.log(`  [${idx}]: ${colName} (column ${String.fromCharCode('A'.charCodeAt(0) + idx)})`);
  });

  // Debug project record to help with troubleshooting
  console.log('[SHEETS_SYNC] Project record fields available:');
  Object.keys(record).forEach(key => {
    const value = record[key];
    const displayValue = typeof value === 'object' ? '[Object]' : value;
    console.log(`  - ${key}: ${displayValue}`);
  });

  // Create an array with the exact length of EXPECTED_COLUMN_STRUCTURE
  // and fill it with empty values initially
  const rowData = Array(EXPECTED_COLUMN_STRUCTURE.length).fill('');
  
  // Map project fields to the correct column positions
  // First 13 columns are project data
  const projectFields = [
    { column: "id", value: record.id || '' },
    { column: "level", value: record.level || '' },
    { column: "projectName", value: record.projectName || '' },
    { column: "Test", value: record.test || '' }, // Test column - placeholder for future use
    { column: "projectDescription", value: record.projectDescription || '' },
    { column: "projectVision", value: record.projectVision || '' },
    { column: "projectLinks", value: record.projectLinks || '' },
    { column: "referralSource", value: record.referralSource || '' },
    { column: "scientificReferences", value: record.scientificReferences || '' },
    { column: "credentialLinks", value: record.credentialLinks || '' },
    { column: "teamMembers", value: record.teamMembers || '' },
    { column: "motivation", value: record.motivation || '' },
    { column: "progress", value: record.progress || '' }
  ];
  
  // Place each field in the right position based on EXPECTED_COLUMN_STRUCTURE
  projectFields.forEach(field => {
    const index = EXPECTED_COLUMN_STRUCTURE.indexOf(field.column);
    if (index !== -1) {
      rowData[index] = field.value;
      console.log(`[SHEETS_SYNC] Set ${field.column} at position ${index} (column ${String.fromCharCode('A'.charCodeAt(0) + index)})`);
    } else {
      console.warn(`[SHEETS_SYNC] WARNING: Column ${field.column} not found in expected structure.`);
    }
  });

  // Add Discord fields if available
  if (discordRecord) {
    console.log('[SHEETS_SYNC] Adding Discord data. Discord ID:', discordRecord.id);
    
    // Debug Discord record to help with troubleshooting
    console.log('[SHEETS_SYNC] Discord record fields available:');
    Object.keys(discordRecord).forEach(key => {
      const value = discordRecord[key];
      const displayValue = typeof value === 'object' ? '[Object]' : value;
      console.log(`  - ${key}: ${displayValue}`);
    });
    
    // Map Discord fields to the correct columns
    const discordFields = [
      { column: "id (Discord)", value: discordRecord.id || '' },
      { column: "serverId", value: discordRecord.serverId || '' },
      { column: "serverName", value: discordRecord.serverName || '' },
      { column: "invitationUrl", value: discordRecord.invitationUrl || '' },
      { column: "messagesCount", value: discordRecord.messagesCount || 0 },
      { column: "papersShared", value: discordRecord.papersShared || 0 },
      { column: "qualityScore", value: discordRecord.qualityScore || 0 },
      { column: "memberCount", value: discordRecord.memberCount || 0 }
    ];
    
    // Place each Discord field in the right position
    discordFields.forEach(field => {
      const index = EXPECTED_COLUMN_STRUCTURE.indexOf(field.column);
      if (index !== -1) {
        rowData[index] = field.value;
        console.log(`[SHEETS_SYNC] Set ${field.column} at position ${index} (column ${String.fromCharCode('A'.charCodeAt(0) + index)})`);
      } else {
        console.warn(`[SHEETS_SYNC] WARNING: Discord column ${field.column} not found in expected structure.`);
      }
    });
    
    // Verify critical metrics positions
    console.log('[SHEETS_SYNC] Verifying critical Discord metrics positions:');
    const metricsToVerify = ["messagesCount", "papersShared", "qualityScore", "memberCount"];
    
    metricsToVerify.forEach(metric => {
      const index = EXPECTED_COLUMN_STRUCTURE.indexOf(metric);
      console.log(`  ${metric} is at index ${index} (Column ${String.fromCharCode('A'.charCodeAt(0) + index)})`);
      
      // Check if our data is at this position
      if (index !== -1) {
        const fieldData = discordFields.find(f => f.column === metric);
        if (fieldData && rowData[index] === fieldData.value) {
          console.log(`  ✓ ${metric} correctly positioned at index ${index}`);
        } else {
          console.error(`  ✗ ${metric} may be incorrectly positioned. Check the data mapping.`);
        }
      }
    });
  }

  // Add lastActivity timestamp - use the most recent activity time available
  const lastActivityIndex = EXPECTED_COLUMN_STRUCTURE.indexOf("lastActivity");
  if (lastActivityIndex !== -1) {
    // Use Discord's last message timestamp or updatedAt if available
    let lastActivityTime = null;
    
    // Try to get Discord's last activity
    if (discordRecord) {
      if (discordRecord.lastMessageTimestamp) {
        lastActivityTime = new Date(discordRecord.lastMessageTimestamp);
      } else if (discordRecord.updatedAt) {
        lastActivityTime = new Date(discordRecord.updatedAt);
      }
    }
    
    // If no Discord activity, use project's updatedAt
    if (!lastActivityTime && record.updatedAt) {
      lastActivityTime = new Date(record.updatedAt);
    }
    
    // If no timestamps found, use current time
    if (!lastActivityTime) {
      lastActivityTime = new Date();
    }
    
    // Convert to formatted date in Eastern time format
    // Format: "2023-05-15 3:45 PM ET"
    const formattedTime = formatDateWithRelative(lastActivityTime);
    rowData[lastActivityIndex] = formattedTime;
    console.log(`[SHEETS_SYNC] Set lastActivity at position ${lastActivityIndex} (column ${String.fromCharCode('A'.charCodeAt(0) + lastActivityIndex)}) to ${rowData[lastActivityIndex]}`);
  }

  // Verify the final row length matches expected column count
  if (rowData.length !== EXPECTED_COLUMN_STRUCTURE.length) {
    console.error(`[SHEETS_SYNC] WARNING: Row data length (${rowData.length}) does not match expected column count (${EXPECTED_COLUMN_STRUCTURE.length})`);
    
    // SAFETY CHECK: Ensure we never return more columns than expected
    if (rowData.length > EXPECTED_COLUMN_STRUCTURE.length) {
      console.warn(`[SHEETS_SYNC] Trimming excess columns from ${rowData.length} to ${EXPECTED_COLUMN_STRUCTURE.length}`);
      rowData.length = EXPECTED_COLUMN_STRUCTURE.length;
    }
  }

  // Return the complete row with data positioned according to EXPECTED_COLUMN_STRUCTURE
  return rowData;
}

/**
 * Sync a project to Google Sheets (create or update)
 */
export async function syncProjectToSheets(projectId: string): Promise<string> {
  try {
    console.log(`[SHEETS_SYNC] Syncing project ${projectId} to Google Sheets with Discord data`);
    
    // Fetch project data from PostgreSQL - Fix table name case sensitivity with quotes
    const projectResult = await pool.query(
      'SELECT * FROM "Project" WHERE id = $1',
      [projectId]
    );
    
    if (projectResult.rows.length === 0) {
      console.error(`[SHEETS_SYNC] No project found with ID: ${projectId}`);
      return `No project found with ID: ${projectId}`;
    }
    
    const projectRecord = projectResult.rows[0];
    console.log(`[SHEETS_SYNC] Found project: ${projectRecord.projectName} (${projectRecord.id})`);
    
    // Fetch associated Discord data (if any)
    let discordRecord = null;
    const discordResult = await pool.query(
      'SELECT * FROM "Discord" WHERE "projectId" = $1 LIMIT 1',
      [projectId]
    );
    
    if (discordResult.rows.length > 0) {
      discordRecord = discordResult.rows[0];
      console.log(`[SHEETS_SYNC] Found associated Discord server: ${discordRecord.serverName} (${discordRecord.id}) for project ${projectId}`);
      console.log(`[SHEETS_SYNC] Discord stats: messages=${discordRecord.messagesCount}, papers=${discordRecord.papersShared}, members=${discordRecord.memberCount}, quality=${discordRecord.qualityScore}`);
    } else {
      console.log(`[SHEETS_SYNC] No Discord server found for project ${projectId}`);
    }
    
    // Get Google Sheets access token
    console.log(`[SHEETS_SYNC] Getting Google access token...`);
    let accessToken;
    try {
      accessToken = await getGoogleAccessToken();
      console.log(`[SHEETS_SYNC] Successfully got Google access token`);
    } catch (tokenError) {
      console.error(`[SHEETS_SYNC] Failed to get Google access token:`, tokenError);
      return `Failed to sync: Could not get Google access token. ${tokenError instanceof Error ? tokenError.message : String(tokenError)}`;
    }

    // Validate spreadsheet structure before any operations - always force refresh to ensure accuracy
    try {
      console.log(`[SHEETS_SYNC] Validating spreadsheet structure...`);
      await validateSpreadsheetColumns(accessToken, true);
      console.log(`[SHEETS_SYNC] Spreadsheet validation completed successfully.`);
    } catch (validationError) {
      console.error(`[SHEETS_SYNC] Spreadsheet validation failed:`, validationError);
      return `Failed to sync: Spreadsheet structure validation failed. ${validationError instanceof Error ? validationError.message : String(validationError)}`;
    }
    
    // Format the project data with Discord data for Google Sheets
    const rowData = formatProjectToRow(projectRecord, discordRecord);
    if (rowData.length === 0) {
      console.error(`[SHEETS_SYNC] Formatted project data is empty for ID: ${projectId}`);
      return `Formatted project data is empty for ID: ${projectId}. Sync skipped.`;
    }
    
    console.log(`[SHEETS_SYNC] Data formatted for Google Sheets with ${rowData.length} columns`);
    
    // Try to find if the project already exists in the sheet
    console.log(`[SHEETS_SYNC] Checking if project exists in sheet using ID: ${projectId}`);
    const rowIndex = await findRowIndexById(accessToken, projectId);
    
    if (rowIndex !== null) {
      // Row found, update it
      const lastColumnLetter = String.fromCharCode('A'.charCodeAt(0) + EXPECTED_COLUMN_STRUCTURE.length - 1);
      const updateRange = `${SHEET_NAME}!A${rowIndex}:${lastColumnLetter}${rowIndex}`;
      
      console.log(`[SHEETS_SYNC] Updating row ${rowIndex} (Range: ${updateRange}) for project ID: ${projectId}`);
      console.log(`[SHEETS_SYNC] Row data to update (column count: ${rowData.length}):`);
      
      // Log detailed information about what we're sending to the API
      // (limit to first few columns to avoid log spam)
      const columnsToLog = Math.min(rowData.length, EXPECTED_COLUMN_STRUCTURE.length);
      for (let i = 0; i < columnsToLog; i++) {
        const columnLetter = String.fromCharCode('A'.charCodeAt(0) + i);
        console.log(`  ${columnLetter}${rowIndex}: ${rowData[i]}`);
      }
      
      try {
        // Safety check - get headers one more time to verify structure before update
        const headerCheckUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:${lastColumnLetter}1`;
        const headerResponse = await axios.get(headerCheckUrl, {
          headers: { Authorization: `Bearer ${accessToken}` }
        });
        
        const headerRow = headerResponse.data.values?.[0] || [];
        console.log(`[SHEETS_SYNC] Header row for final safety check:`, headerRow);
        
        // Check critical metrics columns to ensure they're where we expect
        if (discordRecord) {
          const metricsToCheck = ["messagesCount", "papersShared", "qualityScore", "memberCount"];
          let hasColumnMismatch = false;
          
          for (const metric of metricsToCheck) {
            const expectedIndex = EXPECTED_COLUMN_STRUCTURE.indexOf(metric);
            if (expectedIndex === -1) continue;
            
            const headerValue = headerRow[expectedIndex];
            if (!headerValue || !headerValue.toLowerCase().includes(metric.toLowerCase())) {
              console.error(`[SHEETS_SYNC] SAFETY CHECK: Critical column mismatch for ${metric}! Expected at index ${expectedIndex} but found "${headerValue}"`);
              hasColumnMismatch = true;
            }
          }
          
          if (hasColumnMismatch) {
            console.error(`[SHEETS_SYNC] CRITICAL: Header row doesn't match expected structure. Forcing spreadsheet validation on next operation.`);
            forceRevalidation = true;
            return `Update aborted: Column structure mismatch detected. Please check spreadsheet headers.`;
          }
        }
        
        // Before sending the update, check the current data to debug discrepancies
        const checkCurrentDataUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${updateRange}`;
        const currentDataResponse = await axios.get(
          checkCurrentDataUrl, 
          { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        
        console.log(`[SHEETS_SYNC] Current data in sheet before update:`);
        const currentValues = currentDataResponse.data.values?.[0] || [];
        for (let i = 0; i < Math.min(currentValues.length, EXPECTED_COLUMN_STRUCTURE.length); i++) {
          const columnLetter = String.fromCharCode('A'.charCodeAt(0) + i);
          const columnName = EXPECTED_COLUMN_STRUCTURE[i] || `Column ${columnLetter}`;
          console.log(`  ${columnLetter}${rowIndex} (${columnName}): ${currentValues[i] || '(empty)'}`);
        }
        
        // Verify critical metrics positions for updating
        if (discordRecord) {
          console.log('[SHEETS_SYNC] Critical Discord metrics positions before update:');
          const metricsToCheck = ["messagesCount", "papersShared", "qualityScore", "memberCount"];
          
          metricsToCheck.forEach(metric => {
            const index = EXPECTED_COLUMN_STRUCTURE.indexOf(metric);
            if (index !== -1) {
              console.log(`  Current ${metric}: ${currentValues[index] || '(empty)'} at index ${index} (Column ${String.fromCharCode('A'.charCodeAt(0) + index)})`);
              console.log(`  New ${metric}: ${rowData[index] || '(empty)'}`);
            } else {
              console.error(`  Missing column: ${metric} not found in expected column structure!`);
            }
          });
        }
        
        // Ensure row data is exactly the right length
        if (rowData.length !== EXPECTED_COLUMN_STRUCTURE.length) {
          console.error(`[SHEETS_SYNC] WARNING: Row data length (${rowData.length}) doesn't match expected column count (${EXPECTED_COLUMN_STRUCTURE.length}). Adjusting...`);
          // Pad or trim to match expected structure
          while (rowData.length < EXPECTED_COLUMN_STRUCTURE.length) rowData.push('');
          if (rowData.length > EXPECTED_COLUMN_STRUCTURE.length) rowData.length = EXPECTED_COLUMN_STRUCTURE.length;
        }
        
        // With improved confidence in our data, proceed with the update - use consistent RAW value input
        const updateUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${updateRange}?valueInputOption=RAW`;
        await axios.put(
          updateUrl,
          { values: [rowData] },
          { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        
        console.log(`[SHEETS_SYNC] Successfully updated row ${rowIndex} in sheet for project ID: ${projectId}`);
        
        // Verify the update was successful by fetching the data again
        const verifyUpdateResponse = await axios.get(
          checkCurrentDataUrl, 
          { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        
        console.log(`[SHEETS_SYNC] Verified data after update:`);
        const updatedValues = verifyUpdateResponse.data.values?.[0] || [];
        for (let i = 0; i < Math.min(updatedValues.length, EXPECTED_COLUMN_STRUCTURE.length); i++) {
          const columnLetter = String.fromCharCode('A'.charCodeAt(0) + i);
          const columnName = EXPECTED_COLUMN_STRUCTURE[i] || `Column ${columnLetter}`;
          console.log(`  ${columnLetter}${rowIndex} (${columnName}): ${updatedValues[i] || '(empty)'}`);
        }
        
        // Verify metrics were updated correctly
        if (discordRecord) {
          console.log('[SHEETS_SYNC] Verifying Discord metrics after update:');
          
          const metricsToVerify = ["messagesCount", "papersShared", "qualityScore", "memberCount"];
          
          metricsToVerify.forEach(metric => {
            const index = EXPECTED_COLUMN_STRUCTURE.indexOf(metric);
            if (index !== -1) {
              const expectedValue = discordRecord[metric] || 0;
              const actualValue = updatedValues[index] || 0;
              
              console.log(`  Updated ${metric}: ${actualValue} at index ${index} (Column ${String.fromCharCode('A'.charCodeAt(0) + index)})`);
              
              if (String(expectedValue) !== String(actualValue)) {
                console.error(`  ✗ ${metric} didn't update correctly! Expected ${expectedValue}, got ${actualValue}`);
                // Schedule revalidation for next operation
                forceRevalidation = true;
              } else {
                console.log(`  ✓ ${metric} correctly updated to ${actualValue}`);
              }
            } else {
              console.error(`  Missing column: ${metric} not found in expected column structure!`);
            }
          });
        }
        
        return `Updated row ${rowIndex} in sheet for project ID: ${projectId} with Discord data`;
      } catch (updateError) {
        console.error(`[SHEETS_SYNC] Error updating Google Sheet row:`, updateError);
        if (axios.isAxiosError(updateError) && updateError.response) {
          console.error(`[SHEETS_SYNC] API response status: ${updateError.response.status}`);
          console.error(`[SHEETS_SYNC] API response data:`, updateError.response.data);
        }
        // Schedule revalidation for next operation
        forceRevalidation = true;
        return `Failed to update row ${rowIndex}: ${updateError instanceof Error ? updateError.message : String(updateError)}`;
      }
    } else {
      // Row not found, create it
      console.log(`[SHEETS_SYNC] Row for project ID ${projectId} not found. Creating new row.`);
      console.log(`[SHEETS_SYNC] New row data to append (column count: ${rowData.length}):`);
      
      // Log detailed information about what we're sending to the API
      const columnsToLog = Math.min(rowData.length, EXPECTED_COLUMN_STRUCTURE.length);
      for (let i = 0; i < columnsToLog; i++) {
        const columnLetter = String.fromCharCode('A'.charCodeAt(0) + i);
        const columnName = EXPECTED_COLUMN_STRUCTURE[i] || `Column ${columnLetter}`;
        console.log(`  ${columnLetter} (${columnName}): ${rowData[i]}`);
      }
      
      try {
        // Create a row with the header first if sheet is empty
        let createHeaderFirst = false;
        
        try {
          // Check if sheet needs headers (for first time setup)
          const headersCheckUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:U1`;
          const headersResponse = await axios.get(
            headersCheckUrl, 
            { headers: { Authorization: `Bearer ${accessToken}` } }
          );
          
          if (!headersResponse.data.values || headersResponse.data.values.length === 0) {
            console.log(`[SHEETS_SYNC] Sheet appears to be empty. Will create headers first.`);
            createHeaderFirst = true;
          } else {
            console.log(`[SHEETS_SYNC] Sheet already has headers: ${JSON.stringify(headersResponse.data.values[0])}`);
          }
        } catch (headerCheckError) {
          console.log(`[SHEETS_SYNC] Error checking headers, assuming they exist:`, headerCheckError);
        }
        
        // If needed, add headers first
        if (createHeaderFirst) {
          const headerUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:T1?valueInputOption=USER_ENTERED`;
          await axios.put(
            headerUrl,
            { values: [EXPECTED_COLUMN_STRUCTURE] },
            { headers: { Authorization: `Bearer ${accessToken}` } }
          );
          console.log(`[SHEETS_SYNC] Created headers row in sheet using expected column structure`);
        }
        
        // NEW: Use the correct endpoint for appending rows (/:spreadsheetId/values/:range:append)
        const lastColumnLetter = String.fromCharCode('A'.charCodeAt(0) + EXPECTED_COLUMN_STRUCTURE.length - 1);
        
        // NEW: Use the correct endpoint for appending rows (/:spreadsheetId/values/:range:append)
        const appendUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:${lastColumnLetter}1:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS`;
        
        console.log(`[SHEETS_SYNC] Using append URL: ${appendUrl} with rowData length ${rowData.length}`);
        
        // Detailed logging of final row data
        console.log(`[SHEETS_SYNC] Final row data (${rowData.length} columns):`);
        for (let i = 0; i < rowData.length; i++) {
          const columnLetter = String.fromCharCode('A'.charCodeAt(0) + i);
          const columnName = EXPECTED_COLUMN_STRUCTURE[i] || `Column ${columnLetter}`;
          console.log(`  ${columnLetter} (${columnName}): ${rowData[i]}`);
        }
        
        // Ensure we don't send more data than expected
        if (rowData.length > EXPECTED_COLUMN_STRUCTURE.length) {
          console.warn(`[SHEETS_SYNC] Trimming row data from ${rowData.length} to ${EXPECTED_COLUMN_STRUCTURE.length} columns`);
          rowData.length = EXPECTED_COLUMN_STRUCTURE.length;
        }
        
        const appendResponse = await axios.post(
          appendUrl,
          { values: [rowData] },
          { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        
        // Extract the updated range to show which row was actually updated
        const updatedRange = appendResponse.data?.updates?.updatedRange || 'Unknown range';
        console.log(`[SHEETS_SYNC] Successfully created new row in sheet: ${updatedRange}`);
        
        // Additional check to verify row was created correctly
        const rowRangeMatch = updatedRange.match(/(\d+):/);
        if (rowRangeMatch && rowRangeMatch[1]) {
          const newRowIndex = parseInt(rowRangeMatch[1], 10);
          console.log(`[SHEETS_SYNC] Verifying new row at index ${newRowIndex}`);
          
          try {
            const verifyUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${SHEET_NAME}!A${newRowIndex}:${lastColumnLetter}${newRowIndex}`;
            const verifyResponse = await axios.get(
              verifyUrl,
              { headers: { Authorization: `Bearer ${accessToken}` } }
            );
            
            const verifyValues = verifyResponse.data.values?.[0] || [];
            console.log(`[SHEETS_SYNC] New row verification contents (${verifyValues.length} columns):`);
            for (let i = 0; i < Math.min(verifyValues.length, EXPECTED_COLUMN_STRUCTURE.length); i++) {
              const columnLetter = String.fromCharCode('A'.charCodeAt(0) + i);
              const columnName = EXPECTED_COLUMN_STRUCTURE[i] || `Column ${columnLetter}`;
              console.log(`  ${columnLetter}${newRowIndex} (${columnName}): ${verifyValues[i] || '(empty)'}`);
            }
            
            // Verify key metrics were created correctly
            if (discordRecord) {
              console.log('[SHEETS_SYNC] Verifying Discord metrics in new row:');
              
              const metricsToVerify = ["messagesCount", "papersShared", "qualityScore", "memberCount"];
              
              metricsToVerify.forEach(metric => {
                const index = EXPECTED_COLUMN_STRUCTURE.indexOf(metric);
                if (index !== -1) {
                  const expectedValue = discordRecord[metric] || 0;
                  const actualValue = verifyValues[index] || 0;
                  
                  console.log(`  New row ${metric}: ${actualValue} at index ${index} (Column ${String.fromCharCode('A'.charCodeAt(0) + index)})`);
                  
                  if (String(expectedValue) !== String(actualValue)) {
                    console.error(`  ✗ ${metric} may not be correct! Expected ${expectedValue}, got ${actualValue}`);
                  } else {
                    console.log(`  ✓ ${metric} correctly positioned with value ${actualValue}`);
                  }
                } else {
                  console.error(`  Missing column: ${metric} not found in expected column structure!`);
                }
              });
            }
            
            // Check if there are unexpected extra columns
            if (verifyValues.length > EXPECTED_COLUMN_STRUCTURE.length) {
              console.error(`[SHEETS_SYNC] DETECTED ISSUE: Row has ${verifyValues.length} columns but expected only ${EXPECTED_COLUMN_STRUCTURE.length}`);
              console.log('[SHEETS_SYNC] Extra columns found:');
              for (let i = EXPECTED_COLUMN_STRUCTURE.length; i < verifyValues.length; i++) {
                const columnLetter = String.fromCharCode('A'.charCodeAt(0) + i);
                console.error(`  Extra column ${columnLetter}: "${verifyValues[i]}"`);
              }
            } else {
              console.log(`[SHEETS_SYNC] Column count verification passed: ${verifyValues.length} columns as expected.`);
            }
          } catch (verifyError) {
            console.error(`[SHEETS_SYNC] Failed to verify new row:`, verifyError);
          }
        }
        
        return `Created new row in sheet for project ID: ${projectId} with Discord data at ${updatedRange}`;
      } catch (appendError) {
        console.error(`[SHEETS_SYNC] Error creating new row in Google Sheet:`, appendError);
        if (axios.isAxiosError(appendError) && appendError.response) {
          console.error(`[SHEETS_SYNC] API response status: ${appendError.response.status}`);
          console.error(`[SHEETS_SYNC] API response data:`, appendError.response.data);
        }
        return `Failed to create new row: ${appendError instanceof Error ? appendError.message : String(appendError)}`;
      }
    }
  } catch (error) {
    console.error(`[SHEETS_SYNC] Error syncing project ${projectId} to sheets:`, error);
    // Schedule revalidation for next operation
    forceRevalidation = true;
    return `Failed to sync project to Google Sheets: ${error instanceof Error ? error.message : String(error)}`;
  }
}

/**
 * Sync a Discord server to Google Sheets (create or update)
 */
export async function syncDiscordToSheets(discordId: string): Promise<string> {
  try {
    console.log(`Syncing Discord ${discordId} to Google Sheets by updating project row`);
    
    // Fetch Discord data from PostgreSQL - Fix table name case sensitivity with quotes
    const discordResult = await pool.query(
      'SELECT * FROM "Discord" WHERE id = $1',
      [discordId]
    );
    
    if (discordResult.rows.length === 0) {
      return `No Discord found with ID: ${discordId}`;
    }
    
    const discordRecord = discordResult.rows[0];
    
    // Get the associated project ID
    const projectId = discordRecord.projectId;
    if (!projectId) {
      return `Discord ${discordId} has no associated project_id. Cannot sync to project row.`;
    }
    
    // Sync by calling the project sync function with this Discord data
    return await syncProjectToSheets(projectId);
  } catch (error) {
    console.error(`Error syncing Discord ${discordId} to sheets:`, error);
    throw new Error(`Failed to sync Discord to Google Sheets: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Setup database triggers and functions for automatic syncing
 */
export async function setupDatabaseTriggers(): Promise<void> {
  try {
    console.log('Setting up database triggers for Google Sheets sync - SKIPPED to avoid DB schema changes');
    // This function is intentionally left empty to avoid modifying the database schema
    // For actual setup of database triggers, use the dedicated file: src/services/sheets-db-setup.service.ts
  } catch (error) {
    console.error('Error in setupDatabaseTriggers (skipped):', error);
    throw new Error(`Failed to setup database triggers: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Process the sync queue in batches
 */
export async function processQueue(batchSize: number = 10): Promise<string> {
  try {
    console.log(`Processing Google Sheets sync queue (batch size: ${batchSize}) - SKIPPED to avoid DB interactions`);
    return 'Queue processing is disabled. Use the dedicated service in sheets-db-setup.service.ts if needed.';
  } catch (error) {
    console.error('Error in processQueue (skipped):', error);
    throw new Error(`Failed to process sync queue: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Manually trigger sync for a specific project or discord
 */
export async function manualSync(type: 'project' | 'discord', id: string): Promise<string> {
  try {
    if (type === 'project') {
      return await syncProjectToSheets(id);
    } else if (type === 'discord') {
      // For Discord, we'll find the associated project and sync that
      const discordResult = await pool.query(
        'SELECT "projectId" FROM "Discord" WHERE id = $1',
        [id]
      );
      
      if (discordResult.rows.length === 0 || !discordResult.rows[0].projectId) {
        return `No project associated with Discord ID: ${id}. Cannot sync.`;
      }
      
      const projectId = discordResult.rows[0].projectId;
      return await syncProjectToSheets(projectId);
    } else {
      return 'Invalid type. Must be "project" or "discord".';
    }
  } catch (error) {
    console.error(`Error in manual sync (${type} ${id}):`, error);
    throw new Error(`Manual sync failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Initialize the Google Sheet with all existing projects and their Discord stats
 * This is useful for the first run or to completely refresh the sheet
 */
export async function initializeSheetWithAllProjects(): Promise<string> {
  try {
    console.log('Initializing Google Sheet with all existing projects and their Discord stats');
    
    // Get all projects from the database - removed ORDER BY "createdAt"
    const projectsResult = await pool.query('SELECT id FROM "Project"');
    
    if (projectsResult.rows.length === 0) {
      return 'No projects found in the database to sync';
    }
    
    console.log(`Found ${projectsResult.rows.length} projects to sync`);
    
    let successCount = 0;
    let errorCount = 0;
    let errorDetails: string[] = [];
    
    // Sync each project to the Google Sheet
    for (const project of projectsResult.rows) {
      try {
        console.log(`Syncing project ${project.id}...`);
        await syncProjectToSheets(project.id);
        successCount++;
      } catch (error) {
        errorCount++;
        const errorMessage = error instanceof Error ? error.message : String(error);
        console.error(`Error syncing project ${project.id}:`, error);
        errorDetails.push(`Project ${project.id}: ${errorMessage}`);
      }
    }
    
    // Include first 3 error details in the result message (if any)
    let errorSummary = '';
    if (errorDetails.length > 0) {
      const detailsToShow = errorDetails.slice(0, 3);
      errorSummary = `\nFirst ${detailsToShow.length} errors: \n` + detailsToShow.join('\n');
      if (errorDetails.length > 3) {
        errorSummary += `\n...and ${errorDetails.length - 3} more errors.`;
      }
    }
    
    return `Initialized sheet with ${successCount} projects (${errorCount} errors)${errorSummary}`;
  } catch (error) {
    console.error('Error initializing sheet with all projects:', error);
    throw new Error(`Failed to initialize sheet: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Validates the spreadsheet column structure against expected columns
 * Returns column indices mapping if valid, or throws an error if invalid
 */
async function validateSpreadsheetColumns(accessToken: string, forceRefresh = false): Promise<Record<string, number>> {
  console.log(`[SHEETS_VALIDATE] Validating spreadsheet column structure...`);
  
  // Return cached validation if available and not forced to refresh
  if (validatedColumnIndices && !forceRefresh && !forceRevalidation) {
    console.log(`[SHEETS_VALIDATE] Using cached column validation.`);
    return validatedColumnIndices;
  }
  
  // Reset force revalidation flag
  forceRevalidation = false;
  
  try {
    // Fetch the header row - use a wider range to account for potentially more columns than expected
    const maxColumnLetter = String.fromCharCode('A'.charCodeAt(0) + Math.max(EXPECTED_COLUMN_STRUCTURE.length, 25));
    const headerRange = `${SHEET_NAME}!A1:${maxColumnLetter}1`;
    const url = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${headerRange}`;
    
    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${accessToken}` }
    });
    
    const headerRow = response.data.values?.[0];
    if (!headerRow || headerRow.length === 0) {
      throw new Error(`Header row not found in spreadsheet. Sheet may be empty.`);
    }
    
    console.log(`[SHEETS_VALIDATE] Found header row with ${headerRow.length} columns.`);
    console.log(`[SHEETS_VALIDATE] Header row: ${JSON.stringify(headerRow)}`);
    
    // Create a mapping of column names to indices
    const columnIndices: Record<string, number> = {};
    let hasMissingColumns = false;
    let missingColumns: string[] = [];
    let hasColumnOrderMismatch = false;
    let columnOrderMismatches: string[] = [];
    
    // Check each expected column exists
    EXPECTED_COLUMN_STRUCTURE.forEach((expectedColumn, expectedIndex) => {
      const actualIndex = headerRow.findIndex(
        (header: string) => header && header.trim().toLowerCase() === expectedColumn.toLowerCase()
      );
      
      if (actualIndex === -1) {
        hasMissingColumns = true;
        missingColumns.push(expectedColumn);
        console.error(`[SHEETS_VALIDATE] Missing expected column: ${expectedColumn}`);
      } else {
        if (actualIndex !== expectedIndex) {
          hasColumnOrderMismatch = true;
          columnOrderMismatches.push(`${expectedColumn} (expected at ${expectedIndex}, found at ${actualIndex})`);
          console.warn(`[SHEETS_VALIDATE] Column ${expectedColumn} found at index ${actualIndex} but expected at ${expectedIndex}`);
        }
        columnIndices[expectedColumn] = actualIndex;
      }
    });
    
    // If there are missing columns, add them to the spreadsheet
    if (hasMissingColumns) {
      console.log(`[SHEETS_VALIDATE] Found ${missingColumns.length} missing columns: ${missingColumns.join(', ')}`);
      console.log(`[SHEETS_VALIDATE] Adding missing columns to the spreadsheet...`);
      
      // Get the current last column index
      const lastColIndex = headerRow.length;
      const lastColLetter = String.fromCharCode('A'.charCodeAt(0) + lastColIndex);
      
      // Prepare the new columns data
      const newColumnsData = missingColumns.map((column, index) => {
        const colIndex = lastColIndex + index;
        columnIndices[column] = colIndex; // Update indices map with new columns
        
        return {
          column,
          index: colIndex,
          letter: String.fromCharCode('A'.charCodeAt(0) + colIndex)
        };
      });
      
      // Log what we're adding
      newColumnsData.forEach(col => {
        console.log(`[SHEETS_VALIDATE] Adding column "${col.column}" at position ${col.index} (Column ${col.letter})`);
      });
      
      // Add the columns one by one
      for (const col of newColumnsData) {
        const updateUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${SHEET_NAME}!${col.letter}1?valueInputOption=RAW`;
        
        try {
          await axios.put(
            updateUrl,
            { values: [[col.column]] },
            { headers: { Authorization: `Bearer ${accessToken}` } }
          );
          console.log(`[SHEETS_VALIDATE] Successfully added column "${col.column}" at ${col.letter}1`);
        } catch (addError) {
          console.error(`[SHEETS_VALIDATE] Error adding column "${col.column}":`, addError);
          throw new Error(`Failed to add missing column ${col.column}: ${addError instanceof Error ? addError.message : String(addError)}`);
        }
      }
      
      console.log(`[SHEETS_VALIDATE] Successfully added all missing columns`);
      
      // Re-fetch the header row after adding columns to get the updated data
      console.log(`[SHEETS_VALIDATE] Re-fetching header row to include newly added columns...`);
      const newLastColumnLetter = String.fromCharCode('A'.charCodeAt(0) + lastColIndex + missingColumns.length - 1);
      const updatedHeaderRange = `${SHEET_NAME}!A1:${newLastColumnLetter}1`;
      const updatedHeaderUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${updatedHeaderRange}`;
      
      try {
        const updatedResponse = await axios.get(updatedHeaderUrl, {
          headers: { Authorization: `Bearer ${accessToken}` }
        });
        
        const updatedHeaderRow = updatedResponse.data.values?.[0];
        if (updatedHeaderRow && updatedHeaderRow.length > headerRow.length) {
          // Replace the original header row with the updated one
          headerRow.length = 0; // Clear the original array
          headerRow.push(...updatedHeaderRow); // Add all elements from updated array
          console.log(`[SHEETS_VALIDATE] Updated header row with ${headerRow.length} columns`);
          console.log(`[SHEETS_VALIDATE] New header row: ${JSON.stringify(headerRow)}`);
        } else {
          console.warn(`[SHEETS_VALIDATE] Warning: Updated header row doesn't seem to include new columns`);
        }
      } catch (refetchError) {
        console.error(`[SHEETS_VALIDATE] Error re-fetching header row after adding columns:`, refetchError);
        // Continue with validation using cached indices, but log the issue
        console.warn(`[SHEETS_VALIDATE] Continuing with cached column indices despite refetch error`);
      }
    }
    
    if (hasColumnOrderMismatch) {
      console.warn(`[SHEETS_VALIDATE] Column order mismatches detected: ${columnOrderMismatches.join('; ')}`);
      console.warn(`[SHEETS_VALIDATE] Using actual column positions instead of expected positions.`);
    }
    
    // If we added missing columns, recalculate column indices using the updated header row
    if (hasMissingColumns) {
      console.log(`[SHEETS_VALIDATE] Recalculating column indices with updated header row...`);
      
      // Clear and rebuild columnIndices mapping with the updated header
      Object.keys(columnIndices).forEach(key => delete columnIndices[key]);
      
      EXPECTED_COLUMN_STRUCTURE.forEach((expectedColumn, expectedIndex) => {
        const actualIndex = headerRow.findIndex(
          (header: string) => header && header.trim().toLowerCase() === expectedColumn.toLowerCase()
        );
        
        if (actualIndex !== -1) {
          columnIndices[expectedColumn] = actualIndex;
        } else {
          console.error(`[SHEETS_VALIDATE] Still missing column after update: ${expectedColumn}`);
        }
      });
      
      console.log(`[SHEETS_VALIDATE] Recalculated column indices:`, columnIndices);
    }
    
    // Map critical Discord metrics to their validated indices and verify positions
    const discordMetricIndices = {
      'messagesCount': columnIndices['messagesCount'],
      'papersShared': columnIndices['papersShared'],
      'qualityScore': columnIndices['qualityScore'],
      'memberCount': columnIndices['memberCount'],
      'lastActivity': columnIndices['lastActivity']
    };
    
    console.log(`[SHEETS_VALIDATE] Discord metrics column mapping:`, discordMetricIndices);
    
    // Verify critical column positions - these are our most important columns for Discord data
    console.log('[SHEETS_VALIDATE] Critical Discord column positions:');
    for (const metric of Object.keys(discordMetricIndices)) {
      const columnIndex = columnIndices[metric];
      
      if (columnIndex === undefined) {
        console.error(`[SHEETS_VALIDATE] CRITICAL ERROR: Column index not found for metric: ${metric}`);
        forceRevalidation = true;
        throw new Error(`Critical error: Column index not found for metric: ${metric}`);
      }
      
      const columnLetter = String.fromCharCode('A'.charCodeAt(0) + columnIndex);
      console.log(`  ${metric}: Column ${columnLetter} (index ${columnIndex})`);
      
      // Sanity check - ensure the actual column header at this position contains our metric name
      const actualHeader = headerRow[columnIndex];
      console.log(`  Checking header at index ${columnIndex}: "${actualHeader}"`);
      
      if (!actualHeader) {
        console.error(`[SHEETS_VALIDATE] CRITICAL MISMATCH: Column ${columnLetter} (index ${columnIndex}) is empty/undefined for metric ${metric}`);
        console.error(`[SHEETS_VALIDATE] Header row length: ${headerRow.length}, trying to access index: ${columnIndex}`);
        console.error(`[SHEETS_VALIDATE] Full header row: ${JSON.stringify(headerRow)}`);
        forceRevalidation = true; 
        throw new Error(`Critical column mismatch: Column ${columnLetter} is empty for metric ${metric}. Header row length: ${headerRow.length}, accessed index: ${columnIndex}`);
      } else if (!actualHeader.toLowerCase().includes(metric.toLowerCase())) {
        console.error(`[SHEETS_VALIDATE] CRITICAL MISMATCH: Column ${columnLetter} is supposed to be ${metric} but found "${actualHeader}" instead`);
        forceRevalidation = true; 
        throw new Error(`Critical column mismatch: Expected ${metric} at column ${columnLetter} but found "${actualHeader}"`);
      } else {
        console.log(`  ✓ Verified column ${columnLetter} contains "${actualHeader}"`);
      }
    }
    
    // Cache validation result
    validatedColumnIndices = columnIndices;
    return columnIndices;
  } catch (error) {
    console.error(`[SHEETS_VALIDATE] Error validating spreadsheet columns:`, error);
    // Invalidate cache on error
    validatedColumnIndices = null;
    throw new Error(`Failed to validate spreadsheet structure: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Directly update specific Discord fields in Google Sheets without fetching all data
 * This is an optimization for frequent updates like message counts or member counts
 */
export async function updateDiscordMetric(
  projectId: string, 
  metricName: 'messagesCount' | 'papersShared' | 'qualityScore' | 'memberCount' | 'lastActivity', 
  value: number | string
): Promise<string> {
  try {
    console.log(`[SHEETS_SYNC] Updating Discord metric "${metricName}" to ${value} for project ${projectId}`);
    
    // Get Google Sheets access token
    console.log(`[SHEETS_SYNC] Getting Google access token...`);
    const accessToken = await getGoogleAccessToken();
    
    // Always validate spreadsheet columns with forced refresh to ensure accuracy
    console.log(`[SHEETS_SYNC] Validating spreadsheet structure before update...`);
    const validatedColumns = await validateSpreadsheetColumns(accessToken, true);
    
    // First, find the row for this project
    console.log(`[SHEETS_SYNC] Finding row for project ID: ${projectId}`);
    let rowIndex = await findRowIndexById(accessToken, projectId);
    
    // If row not found, we need to create it first, then update
    if (rowIndex === null) {
      console.log(`[SHEETS_SYNC] Project ID ${projectId} not found in Google Sheet. Will create row first.`);
      
      try {
        // Look up the project data
        const projectResult = await pool.query(
          'SELECT * FROM "Project" WHERE id = $1',
          [projectId]
        );
        
        if (projectResult.rows.length > 0) {
          console.log(`[SHEETS_SYNC] Found project in database. Creating row in Google Sheet.`);
          
          // Create the row first by calling syncProjectToSheets
          const syncResult = await syncProjectToSheets(projectId);
          console.log(`[SHEETS_SYNC] Row creation result: ${syncResult}`);
          
          // Try to find the row again now that it should exist
          rowIndex = await findRowIndexById(accessToken, projectId);
          
          if (rowIndex === null) {
            console.error(`[SHEETS_SYNC] CRITICAL: Unable to find row even after creation. Manual investigation needed.`);
            return `Critical error: Unable to locate row for project ${projectId} even after creation.`;
          }
          
          console.log(`[SHEETS_SYNC] Successfully created row at index ${rowIndex} for project ${projectId}`);
        } else {
          console.error(`[SHEETS_SYNC] Project ${projectId} not found in database. Cannot create row.`);
          return `Project ID ${projectId} not found in database. Cannot create or update.`;
        }
      } catch (createError) {
        console.error(`[SHEETS_SYNC] Error creating row for project ${projectId}:`, createError);
        return `Failed to create row for project ${projectId}: ${createError instanceof Error ? createError.message : String(createError)}`;
      }
    }
    
    // At this point, we should have a valid rowIndex
    console.log(`[SHEETS_SYNC] Proceeding with metric update at row ${rowIndex}`);
    
    // Use the validated column index from our validation function
    const columnIndex = validatedColumns[metricName];
    if (columnIndex === undefined) {
      return `Column for metric: ${metricName} not found in spreadsheet.`;
    }
    
    // Double-check the column index by fetching the header value in that position
    try {
      const headerCheckUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:U1`;
      const headerResponse = await axios.get(headerCheckUrl, {
        headers: { Authorization: `Bearer ${accessToken}` }
      });
      
      const headerRow = headerResponse.data.values?.[0] || [];
      const headerAtColumn = headerRow[columnIndex];
      
      if (!headerAtColumn || !headerAtColumn.toLowerCase().includes(metricName.toLowerCase())) {
        console.error(`[SHEETS_SYNC] CRITICAL SAFETY CHECK FAILED: Column at index ${columnIndex} contains "${headerAtColumn}", expected ${metricName}`);
        console.error(`[SHEETS_SYNC] Aborting update to prevent data corruption.`);
        // Force revalidation on next operation
        forceRevalidation = true;
        return `Update aborted: Column validation failed. Expected ${metricName} at index ${columnIndex} but found "${headerAtColumn}".`;
      }
      
      console.log(`[SHEETS_SYNC] Safety check passed: Column at index ${columnIndex} contains "${headerAtColumn}" as expected.`);
    } catch (headerCheckError) {
      console.error(`[SHEETS_SYNC] Error checking header row:`, headerCheckError);
      // Continue anyway, we already validated once
    }
    
    // Convert to column letter (A=0, B=1, etc.)
    const columnLetter = String.fromCharCode('A'.charCodeAt(0) + columnIndex);
    const updateRange = `${SHEET_NAME}!${columnLetter}${rowIndex}`;
    
    console.log(`[SHEETS_SYNC] Updating metric "${metricName}" in range ${updateRange} to value: ${value}`);
    console.log(`[SHEETS_SYNC] Using validated column index: ${columnIndex} (${columnLetter})`);
    
    // Update just this specific cell
    try {
      const updateUrl = `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${updateRange}?valueInputOption=RAW`;
      await axios.put(
        updateUrl,
        { values: [[value]] }, // Double array for single cell
        { headers: { Authorization: `Bearer ${accessToken}` } }
      );
      
      // Verify the update
      const verifyUpdateResponse = await axios.get(
        `${GOOGLE_SHEETS_API_ENDPOINT}/${SPREADSHEET_ID}/values/${updateRange}`,
        { headers: { Authorization: `Bearer ${accessToken}` } }
      );
      
      const updatedValue = verifyUpdateResponse.data.values?.[0]?.[0];
      console.log(`[SHEETS_SYNC] Verified update: cell ${updateRange} now contains: ${updatedValue}`);
      
      console.log(`[SHEETS_SYNC] Successfully updated ${metricName} to ${value} for project ${projectId}`);
      return `Updated ${metricName} to ${value} for project ${projectId}`;
    } catch (updateError) {
      console.error(`[SHEETS_SYNC] Error updating ${metricName}:`, updateError);
      if (axios.isAxiosError(updateError) && updateError.response) {
        console.error(`[SHEETS_SYNC] API response status: ${updateError.response.status}`);
        console.error(`[SHEETS_SYNC] API response data:`, updateError.response.data);
      }
      return `Failed to update ${metricName}: ${updateError instanceof Error ? updateError.message : String(updateError)}`;
    }
  } catch (error) {
    console.error(`[SHEETS_SYNC] Error in updateDiscordMetric:`, error);
    return `Error updating Discord metric: ${error instanceof Error ? error.message : String(error)}`;
  }
}

/**
 * Utility function for directly updating message count
 */
export async function updateMessageCount(projectId: string, count: number): Promise<string> {
  return updateDiscordMetric(projectId, 'messagesCount', count);
}

/**
 * Utility function for directly updating papers shared count
 */
export async function updatePapersShared(projectId: string, count: number): Promise<string> {
  return updateDiscordMetric(projectId, 'papersShared', count);
}

/**
 * Utility function for directly updating quality score
 */
export async function updateQualityScore(projectId: string, score: number): Promise<string> {
  return updateDiscordMetric(projectId, 'qualityScore', score);
}

/**
 * Utility function for directly updating member count
 */
export async function updateMemberCount(projectId: string, count: number): Promise<string> {
  return updateDiscordMetric(projectId, 'memberCount', count);
}

/**
 * Utility function for directly updating last activity timestamp
 */
export async function updateLastActivity(projectId: string, timestamp?: Date): Promise<string> {
  const activityTime = timestamp || new Date();
  const formattedTime = formatDateWithRelative(activityTime);
  return updateDiscordMetric(projectId, 'lastActivity', formattedTime);
}

// Helper function to sync Discord stats to Google Sheets - add near the top after function declarations
export async function syncDiscordStatsToSheets(
  guildId: string,
  specificMetric?: 'messagesCount' | 'papersShared' | 'qualityScore' | 'memberCount' | 'lastActivity',
  value?: number | string
): Promise<void> {
  try {
    // Find the Discord record to get the projectId
    const discordRecord = await prisma.discord.findFirst({ 
      where: { serverId: guildId } 
    });
    
    if (!discordRecord?.projectId) {
      console.error(`[SHEETS_SYNC] Cannot sync Discord stats for guild ${guildId}: No project ID found`);
      return;
    }

    const projectId = discordRecord.projectId;
    
    // UPDATE: Always update all metrics together for consistency
    console.log(`[SHEETS_SYNC] Syncing all Discord metrics for project ${projectId} to Google Sheets`);
    
    try {
      // Validate the spreadsheet structure before performing any updates
      console.log(`[SHEETS_SYNC] Validating column structure before Discord stats update`);
      const accessToken = await getGoogleAccessToken();
      await validateSpreadsheetColumns(accessToken, true);
      
      // Update specific metric first if provided, then sync everything
      if (specificMetric && value !== undefined) {
        // Update the specific value in the Discord record
        console.log(`[SHEETS_SYNC] Updating specific metric ${specificMetric}=${value} in Discord record`);
        await prisma.discord.update({
          where: { id: discordRecord.id },
          data: { [specificMetric]: value }
        });
      }
      
      // Always sync the entire project row with all updated metrics
      console.log(`[SHEETS_SYNC] Syncing complete Discord stats to Google Sheets for project ${projectId}`);
      await syncProjectToSheets(projectId).catch(error => {
        console.error(`[SHEETS_SYNC] Error syncing project ${projectId} to Google Sheets:`, error);
      });
    } catch (metricError) {
      console.error(`[SHEETS_SYNC] Error updating Discord stats:`, metricError);
    }
  } catch (error) {
    console.error(`[SHEETS_SYNC] Error in syncDiscordStatsToSheets for guild ${guildId}:`, error);
  }
} 