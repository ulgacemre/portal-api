# Services

This directory contains various service modules for the BioDAO portal API.

## Google Sheets Sync Service

The `sheets-sync.service.ts` handles synchronization between the PostgreSQL database and Google Sheets.

### Recent Fixes (January 2025)

#### Column Validation Error Fix

**Problem**: The system was encountering an error where the `lastActivity` column validation would fail with "CRITICAL MISMATCH: Column V is supposed to be lastActivity but found 'undefined' instead". This occurred because:

1. The validation function would detect missing columns (like `lastActivity`)
2. It would add the missing column to the spreadsheet
3. But when verifying the newly added column, it was still using the old header row data that was fetched before adding the column
4. This caused the verification to find "undefined" instead of the newly added column name

**Solution**: Updated the `validateSpreadsheetColumns` function to:

1. **Re-fetch header row after adding columns**: After adding missing columns, the function now re-fetches the updated header row from Google Sheets
2. **Recalculate column indices**: The column indices mapping is rebuilt using the updated header row
3. **Enhanced error reporting**: Added more detailed logging and error messages to help debug column structure issues
4. **Flexible header range**: Updated the initial header fetch to use a wider range to account for more columns than expected

**Key Changes**:
- Added header row re-fetching after column additions
- Recalculate column indices with updated header data
- Enhanced validation error messages with more context
- Improved logging for debugging column structure issues

### Usage

#### Initialize and Sync Data

The `initialize-sheets.ts` script can handle both structure setup and data synchronization:

```bash
# Full setup + sync all projects (default)
INITIALIZE_SHEET=true npm run initialize-sheets

# Setup sheet structure only (headers, formatting)
INITIALIZE_SHEET=true npm run initialize-sheets -- --setup-only

# Sync all project data only (assumes structure exists)
INITIALIZE_SHEET=true npm run initialize-sheets -- --sync-only

# Sync specific project by ID
INITIALIZE_SHEET=true npm run initialize-sheets -- --project=your-project-id

# Show help
npm run initialize-sheets -- --help
```

#### Testing

Use the test script to verify the Google Sheets validation:

```bash
npm run test:sheets-validation
```

### Column Structure

The expected column structure is defined in the `EXPECTED_COLUMN_STRUCTURE` constant:

```
id, level, projectName, Test, projectDescription, projectVision, projectLinks, 
referralSource, scientificReferences, credentialLinks, teamMembers, motivation, 
progress, id (Discord), serverId, serverName, invitationUrl, messagesCount, 
papersShared, qualityScore, memberCount, lastActivity
```

Critical Discord metrics columns:
- **messagesCount** (Column R, index 17)
- **papersShared** (Column S, index 18) 
- **qualityScore** (Column T, index 19)
- **memberCount** (Column U, index 20)
- **lastActivity** (Column V, index 21) 