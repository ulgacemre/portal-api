/**
 * Google Sheets Initialization Script
 * 
 * This script initializes or refreshes the Google Sheet with project data from the database.
 * 
 * Usage:
 * - To sync all projects: npm run initialize-sheets
 * - To sync a specific project: npm run initialize-sheets -- --project=<project-id>
 * 
 * Environment variables:
 * - INITIALIZE_SHEET: Set to 'true' to run the initialization (safety measure)
 */

import dotenv from 'dotenv';
import { initializeSheetWithAllProjects, syncProjectToSheets } from '../services/sheets-sync.service';
import { getGoogleAccessToken } from '../services/sheets-sync.service';
import axios from 'axios';

// Load environment variables
dotenv.config();

const SPREADSHEET_ID = process.env.GOOGLE_SHEET_ID;
const SHEET_NAME = 'Projects';

// Expected columns structure
const EXPECTED_COLUMNS = [
  "id", "level", "projectName","test", "projectDescription", "projectVision", 
  "projectLinks", "referralSource", "scientificReferences", "credentialLinks", 
  "teamMembers", "motivation", "progress", 
  "id (Discord)", "serverId", "serverName", "invitationUrl",
  "messagesCount", "papersShared", "qualityScore", "memberCount", "lastActivity"
];

/**
 * Initialize Google Sheets for BioDAO project tracking
 * - Verifies spreadsheet access
 * - Creates Projects sheet if it doesn't exist
 * - Sets up column headers
 */
async function initializeSheets(): Promise<void> {
  try {
    console.log('[INITIALIZE] Starting Google Sheets initialization');
    
    if (!SPREADSHEET_ID) {
      throw new Error('GOOGLE_SHEET_ID environment variable is not set');
    }
    
    const accessToken = await getGoogleAccessToken();
    console.log('[INITIALIZE] Successfully obtained Google API access token');
    
    // 1. First verify we can access the spreadsheet
    console.log(`[INITIALIZE] Verifying access to spreadsheet: ${SPREADSHEET_ID}`);
    try {
      const spreadsheetUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}?fields=properties`;
      const spreadsheetResponse = await axios.get(spreadsheetUrl, {
        headers: { Authorization: `Bearer ${accessToken}` }
      });
      
      console.log(`[INITIALIZE] Successfully accessed spreadsheet: "${spreadsheetResponse.data.properties?.title}"`);
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        console.error(`[INITIALIZE] ERROR: Spreadsheet with ID ${SPREADSHEET_ID} not found.`);
        console.error('[INITIALIZE] Please check your GOOGLE_SHEET_ID environment variable.');
        console.error('[INITIALIZE] You may need to create a new spreadsheet and share it with your service account.');
        process.exit(1);
      } else {
        console.error('[INITIALIZE] Error accessing spreadsheet:', error);
        throw error;
      }
    }
    
    // 2. Check if the Projects sheet exists
    console.log(`[INITIALIZE] Checking if sheet "${SHEET_NAME}" exists`);
    let sheetExists = false;
    try {
      const sheetCheckUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:A1`;
      await axios.get(sheetCheckUrl, {
        headers: { Authorization: `Bearer ${accessToken}` }
      });
      sheetExists = true;
      console.log(`[INITIALIZE] Sheet "${SHEET_NAME}" exists`);
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        console.log(`[INITIALIZE] Sheet "${SHEET_NAME}" does not exist - will create it`);
      } else {
        console.error('[INITIALIZE] Error checking if sheet exists:', error);
        throw error;
      }
    }
    
    // 3. Create the sheet if it doesn't exist
    if (!sheetExists) {
      console.log(`[INITIALIZE] Creating sheet "${SHEET_NAME}"`);
      try {
        const createSheetUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`;
        await axios.post(
          createSheetUrl, 
          {
            requests: [
              {
                addSheet: {
                  properties: {
                    title: SHEET_NAME
                  }
                }
              }
            ]
          },
          { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        console.log(`[INITIALIZE] Successfully created sheet "${SHEET_NAME}"`);
      } catch (error) {
        console.error(`[INITIALIZE] Error creating sheet "${SHEET_NAME}":`, error);
        throw error;
      }
    }
    
    // 4. Initialize headers
    console.log('[INITIALIZE] Setting up column headers');
    try {
      const headersUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${SHEET_NAME}!A1:U1?valueInputOption=RAW`;
      await axios.put(
        headersUrl,
        { values: [EXPECTED_COLUMNS] },
        { headers: { Authorization: `Bearer ${accessToken}` } }
      );
      console.log('[INITIALIZE] Successfully set up column headers');
    } catch (error) {
      console.error('[INITIALIZE] Error setting up headers:', error);
      throw error;
    }
    
    // 5. Format headers (make bold, freeze)
    console.log('[INITIALIZE] Formatting headers');
    try {
      const formatHeadersUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`;
      await axios.post(
        formatHeadersUrl,
        {
          requests: [
            // Make first row bold
            {
              repeatCell: {
                range: {
                  sheetId: 0,
                  startRowIndex: 0,
                  endRowIndex: 1
                },
                cell: {
                  userEnteredFormat: {
                    textFormat: {
                      bold: true
                    },
                    backgroundColor: {
                      red: 0.9,
                      green: 0.9,
                      blue: 0.9
                    }
                  }
                },
                fields: "userEnteredFormat(textFormat,backgroundColor)"
              }
            },
            // Freeze the header row
            {
              updateSheetProperties: {
                properties: {
                  sheetId: 0,
                  gridProperties: {
                    frozenRowCount: 1
                  }
                },
                fields: "gridProperties.frozenRowCount"
              }
            }
          ]
        },
        { headers: { Authorization: `Bearer ${accessToken}` } }
      );
      console.log('[INITIALIZE] Successfully formatted headers');
    } catch (error) {
      console.error('[INITIALIZE] Error formatting headers:', error);
      // Non-critical error, continue
    }
    
    console.log('[INITIALIZE] Google Sheets initialization complete!');
    console.log('[INITIALIZE] Your spreadsheet is now ready for BioDAO project tracking.');
    console.log('[INITIALIZE] You can now run the sync scripts to populate data.');
  } catch (error) {
    console.error('[INITIALIZE] Initialization failed:', error);
    process.exit(1);
  }
}

// Run the initialization
if (require.main === module) {
  initializeSheets().catch(console.error);
}

export { initializeSheets }; 