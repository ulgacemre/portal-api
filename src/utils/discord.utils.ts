import axios from 'axios';
import dotenv from 'dotenv';
import prisma from '../services/db.service';

// Initialize environment variables
dotenv.config();


// Discord bot configuration
const DISCORD_BOT_CONFIG = {
  clientId: process.env.DISCORD_CLIENT_ID || '1361285493521907832',
  permissions: '8', // Administrator permissions
  scope: 'bot',
  baseUrl: 'https://discord.com/api/oauth2/authorize',
};

/**
 * Checks Discord-related level progress and performs level-ups if conditions are met
 * @param project - The project to check level progress for
 * @returns True if a level-up occurred, false otherwise
 */
export async function checkDiscordLevelProgress(project: any): Promise<boolean> {
  try {
    if (!project || !project.Discord) {
      console.log(`[Level Check] Project ${project?.id} has no Discord data, skipping check`);
      return false;
    }

    const discord = project.Discord;
    const currentLevel = project.level;
    let leveledUp = false;
    let newLevel = currentLevel;

    // Level 2 to 3: Check if they have Discord server with enough members
    if (currentLevel === 2 && discord.botAdded && discord.memberCount >= 4) {
      console.log(
        `[Level Check] Project ${project.id} meets level 3 requirement: ${discord.memberCount} members`
      );
      leveledUp = true;
      newLevel = 3;
    }
    // Level 3 to 4: Check Discord metrics (members, messages, papers)
    else if (
      currentLevel === 3 &&
      discord.memberCount >= 5 &&
      discord.papersShared >= 5 &&
      discord.messagesCount >= 50
    ) {
      console.log(
        `[Level Check] Project ${project.id} meets level 4 requirements: ` +
          `${discord.memberCount}/5 members, ${discord.papersShared}/5 papers, ${discord.messagesCount}/50 messages`
      );
      leveledUp = true;
      newLevel = 4;
    }

    // If conditions are met, perform the level-up
    if (leveledUp) {
      console.log(
        `[Level Check] Leveling up project ${project.id} from ${currentLevel} to ${newLevel}`
      );

      // Update the project level
      await prisma.project.update({
        where: { id: project.id },
        data: { level: newLevel },
      });

      // If the user has an email, send a level-up email
      if (project.email) {
        try {
          // This would typically call an email service
          console.log(`Level-up email would be sent to ${project.email} for level ${newLevel}`);
        } catch (emailError) {
          console.error('Error sending level-up email:', emailError);
        }
      }

      // If level 4 (sandbox) is reached, send an email to BioDAO team
      if (newLevel === 4) {
        try {
          // This would typically call an email service to notify the team
          console.log(`Sandbox notification email would be sent for project ${project.id}`);
        } catch (emailError) {
          console.error('Error sending sandbox notification email:', emailError);
        }
      }

      return true;
    }

    return false;
  } catch (error) {
    console.error('Error checking Discord level progress:', error);
    return false;
  }
}

/**
 * Checks if the bot is installed on a Discord server
 * @param projectId - The project ID to check bot installation status for
 * @returns Object containing installation status and installation link
 */
export async function checkBotInstallationStatus(
  projectId: string
): Promise<{ installed: boolean; installationLink: string | null }> {
  try {
    // Get Discord record for the project
    const discord = await prisma.discord.findUnique({
      where: { projectId },
    });

    // If no Discord record exists or bot is not added
    if (!discord || !discord.botAdded) {
      // Generate installation link
      const installationLink = `${DISCORD_BOT_CONFIG.baseUrl}?client_id=${DISCORD_BOT_CONFIG.clientId}&permissions=${DISCORD_BOT_CONFIG.permissions}&scope=${DISCORD_BOT_CONFIG.scope}`;

      return {
        installed: false,
        installationLink,
      };
    }

    // Bot is installed
    return {
      installed: true,
      installationLink: null,
    };
  } catch (error) {
    console.error('Error checking bot installation status:', error);

    // Return default values in case of error
    return {
      installed: false,
      installationLink: `${DISCORD_BOT_CONFIG.baseUrl}?client_id=${DISCORD_BOT_CONFIG.clientId}&permissions=${DISCORD_BOT_CONFIG.permissions}&scope=${DISCORD_BOT_CONFIG.scope}`,
    };
  }
}

export function getBotInstallationUrl(): string {
  return `${DISCORD_BOT_CONFIG.baseUrl}?client_id=${DISCORD_BOT_CONFIG.clientId}&permissions=${DISCORD_BOT_CONFIG.permissions}&scope=${DISCORD_BOT_CONFIG.scope}`;
}

/**
 * Extracts Discord server information from a message or invite link
 * @param message - The Discord invite message or link
 * @returns Extracted Discord information
 */
export function extractDiscordInfo(message: string): {
  serverId: string | null;
  inviteLink: string | null;
  inviteCode: string | null;
} {
  const result = {
    serverId: null as string | null,
    inviteLink: null as string | null,
    inviteCode: null as string | null,
  };

  // Check for direct server IDs (rare but possible)
  const serverIdMatch = message.match(/\b(\d{17,20})\b/);
  if (serverIdMatch) {
    result.serverId = serverIdMatch[1];
  }

  // Check for Discord invite links
  const inviteLinkRegex =
    /(https?:\/\/)?(www\.)?(discord\.(gg|io|me|li)|discordapp\.com\/invite)\/([a-zA-Z0-9-]{2,32})/i;
  const inviteMatch = message.match(inviteLinkRegex);

  if (inviteMatch) {
    const fullInviteLink = inviteMatch[0];
    const inviteCode = inviteMatch[5];

    result.inviteLink = fullInviteLink;
    result.inviteCode = inviteCode;
  }

  return result;
}

/**
 * Gets the requirements for the next level based on current level
 * @param currentLevel - The current level of the project
 * @returns Array of requirement strings for the next level
 */
export function getNextLevelRequirements(currentLevel: number): string[] {
  switch (currentLevel) {
    case 1:
      return ['Mint Idea NFT', 'Mint Vision NFT'];
    case 2:
      return ['Create Discord Server', 'Reach 4+ Members'];
    case 3:
      return ['Reach 5+ Members', 'Share 5+ Scientific Papers', 'Send 50+ Messages'];
    case 4:
      return ['All requirements met - Bio team will contact you'];
    default:
      return ['Unknown level'];
  }
}
