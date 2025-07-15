import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import { processMessage } from '../ai';
import prisma, {
  ChatSessionService,
  ChatMessageService,
  ProjectService,
  DiscordService,
  NFTService,
  BioUserService,
} from '../services/db.service';
import TwitterService from '../services/twitter.service';
import { extractDiscordInfo } from '../utils/helpers';
import { mintIdeaNft, mintVisionNft } from '../nft-service';
import { generateIdeaNFTImage, generateVisionNFTImage } from '../image-generation-service';
import { fetchDiscordServerInfo } from '../services/discord.service';
import config from '../config';
import { getBotInstallationUrl } from '../utils/discord.utils';
import { Prisma } from '@prisma/client';

// Map to store active WebSocket connections by user ID
const activeConnections: Record<string, WebSocket> = {};

// Add a map to track servers that have already had bot installation notifications
const botInstallNotificationSent = new Map<string, boolean>();

// Add a map to track recent level-up notifications to prevent duplicates
const recentLevelUpsByUser = new Map<string, Map<number, number>>();

// Session type constants
const SESSION_TYPES = {
  CORE_AGENT: 'coreagent',
  COACHING_AGENT: 'coachingagent'
};

/**
 * Gets an existing chat session or creates a new one for a user
 * @param userId User ID
 * @returns Chat session ID
 */
async function getOrCreateChatSession(userId: string): Promise<string> {
  try {
    // Always check for any existing session, with no time restriction
    const existingSession = await prisma.chatSession.findFirst({
      where: {
        projectId: userId,
        sessionType: SESSION_TYPES.CORE_AGENT
      },
      orderBy: {
        updatedAt: 'desc',
      },
    });

    if (existingSession) {
      // Update the session's updated_at timestamp
      await prisma.chatSession.update({
        where: { id: existingSession.id },
        data: { updatedAt: new Date() },
      });
      console.log(`Using existing chat session ${existingSession.id} for user ${userId}`);
      return existingSession.id;
    }

    // Create a new session only if no previous sessions exist
    const newSession = await prisma.chatSession.create({
      data: {
        projectId: userId,
        sessionType: SESSION_TYPES.CORE_AGENT,
        updatedAt: new Date(),
      },
    });

    console.log(`Created new chat session ${newSession.id} for user ${userId} (first-time user)`);
    return newSession.id;
  } catch (error) {
    console.error('Error managing chat session:', error);
    throw error;
  }
}

/**
 * Saves a chat message to the database
 * @param sessionId Chat session ID
 * @param content Message content (string or object with content property)
 * @param isFromAgent Whether the message is from the agent
 * @param actionTaken Optional action that was taken
 * @param actionSuccess Optional success status of the action
 */
async function saveChatMessage(
  sessionId: string,
  content: string | { content: string },
  isFromAgent: boolean,
  actionTaken?: string,
  actionSuccess?: boolean
): Promise<void> {
  try {
    const messageContent = typeof content === 'string' ? content : content.content;

    await prisma.chatMessage.create({
      data: {
        sessionId,
        content: messageContent,
        isFromAgent,
        actionTaken,
        actionSuccess,
      },
    });
  } catch (error) {
    console.error('Error saving chat message:', error);
    // Don't throw - we don't want to interrupt the user experience if message saving fails
  }
}

/**
 * Initialize WebSocket server
 * @param server HTTP server instance
 * @returns WebSocket server instance
 */
function initWebSocketServer(server: http.Server): WebSocketServer {
  const wss = new WebSocketServer({ server });

  wss.on('connection', (ws) => {
    console.log('Client connected to WebSocket');

    // Handle client messages
    ws.on('message', async (message) => {
      try {
        const data = JSON.parse(message.toString());
        console.log('Received message type:', data.type);

        // Skip messages with coaching_ prefix - these should be handled by the coaching agent only
        if (data.type && data.type.startsWith('coaching_')) {
          console.log(`Ignoring coaching agent message type: ${data.type}`);
          return;
        }

        switch (data.type) {
          case 'auth': {
            await handleAuthentication(ws, data);

            // After authentication, check for pending bot notifications
            // Extract userId from different possible formats
            const userId = data.userId || (data.project && data.project.id);
            if (userId) {
              console.log(`Checking for pending notifications after auth for user ${userId}`);
              //checkForPendingBotNotifications(ws, userId);
            } else {
              console.log(`Cannot check for pending notifications: No user ID in auth message`);
            }
            break;
          }
          case 'message': {
            // Try to extract userId from the message or active connections
            let userId = data.userId; // First try to get from message payload

            if (!userId) {
              // If not in message, find from active connections
              for (const id in activeConnections) {
                if (activeConnections[id] === ws) {
                  userId = id;
                  console.log(`Retrieved userId ${userId} from active connections`);
                  break;
                }
              }
            }

            // Add userId to data for downstream handlers
            if (userId) {
              data.userId = userId;
            }

            if (data.content) {
              await handleMessage(ws, data);
            } else {
              console.error('Invalid message format: missing content');
              ws.send(JSON.stringify({ type: 'error', message: 'Invalid message format' }));
            }
            break;
          }
          case 'get_nfts': {
            // Fetch NFTs for the user from our database
            await handleGetNFTs(ws, data.userId);
            break;
          }
          case 'fetch_discord': {
            // This will send Discord info back to the client if available
            await handleCheckDiscordStats(ws, data.userId);
            break;
          }
          case 'bot_installed': {
            // Handle notification that a Discord bot was installed
            if (data.userId && data.guildId && data.memberCount) {
              await handleBotInstalled(ws, data.userId, {
                guildId: data.guildId,
                guildName: data.guildName,
                memberCount: data.memberCount,
              });
              const latestProject = await ProjectService.getById(data.userId);
              await checkAndPerformLevelUp(latestProject, ws);
            } else {
              console.error('Invalid bot_installed message format');
              ws.send(
                JSON.stringify({
                  type: 'error',
                  message: 'Invalid bot_installed format. Required: userId, guildId, memberCount',
                })
              );
            }
            break;
          }
          case 'handle_discord_bot_callback': {
            // Handle callback from Discord bot installation
            if (data.serverId && data.token) {
              // ToDo: Implement verification logic
              console.log('Discord bot callback received:', data);

              // For now, just confirm the message was received
              ws.send(
                JSON.stringify({
                  type: 'discord_callback_received',
                  success: true,
                })
              );
            }
            break;
          }
          case 'twitter_connect':
            // Handle Twitter account connection
            await handleTwitterConnect(ws, data.userId, {
              twitterId: data.twitterId,
              twitterUsername: data.twitterUsername
            });
            break;
            
          case 'verify_twitter_tweets':
            // Handle verification of Twitter introductory tweets
            await handleVerifyTwitterTweets(ws, data.userId);
            break;

          case 'submit_twitter_tweets':
            // Handle submission of specific tweet URLs for verification
            if (data.userId && data.tweetUrls && Array.isArray(data.tweetUrls)) {
              await handleSubmitTwitterTweets(ws, data.userId, data.tweetUrls);
            } else {
              ws.send(
                JSON.stringify({
                  type: 'error',
                  message: 'Invalid tweet submission format. Required: userId, tweetUrls (array)',
                })
              );
            }
            break;
          case 'get_verified_scientists':
            // Fetch the count of verified scientists for the project
            if (data.userId) {
              await handleGetVerifiedScientists(ws, data.userId);
            } else {
              ws.send(
                JSON.stringify({
                  type: 'error',
                  message: 'User ID is required',
                })
              );
            }
            break;
          case 'verify_twitter_space':
            // Verify a Twitter Space for the project
            if (data.userId && data.spaceUrl) {
              await handleVerifyTwitterSpace(ws, data.userId, data.spaceUrl);
            } else {
              ws.send(
                JSON.stringify({
                  type: 'error',
                  message: 'User ID and Twitter Space URL are required',
                })
              );
            }
            break;
          case 'get_twitter_space_status':
            // Get the status of Twitter Space verification
            if (data.userId) {
              await handleGetTwitterSpaceStatus(ws, data.userId);
            } else {
              ws.send(
                JSON.stringify({
                  type: 'error',
                  message: 'User ID is required',
                })
              );
            }
            break;
          case 'verify_blogpost':
            // Verify a blogpost URL for the project
            if (data.userId && data.blogpostUrl) {
              await handleVerifyBlogpost(ws, data.userId, data.blogpostUrl);
            } else {
              ws.send(
                JSON.stringify({
                  type: 'error',
                  message: 'User ID and blogpost URL are required',
                })
              );
            }
            break;
          case 'verify_twitter_thread':
            // Verify a Twitter thread for the project
            if (data.userId && data.threadUrl) {
              await handleVerifyTwitterThread(ws, data.userId, data.threadUrl);
            } else {
              ws.send(
                JSON.stringify({
                  type: 'error',
                  message: 'User ID and Twitter thread URL are required',
                })
              );
            }
            break;
          case 'get_blogpost_status':
            // Get blogpost verification status
            if (data.userId) {
              await handleGetBlogpostStatus(ws, data.userId);
            } else {
              ws.send(
                JSON.stringify({
                  type: 'error',
                  message: 'User ID is required',
                })
              );
            }
            break;

          default:
            console.log('Unknown message type:', data.type);
        }
      } catch (error) {
        console.error('Error handling WebSocket message:', error);
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid message format' }));
      }
    });

    // Handle client disconnect
    ws.on('close', () => {
      console.log('Client disconnected from WebSocket');
      // Remove this connection from activeConnections
      for (const userId in activeConnections) {
        if (activeConnections[userId] === ws) {
          console.log(`Removing connection for user ${userId}`);
          delete activeConnections[userId];
          break;
        }
      }
    });
  });

  return wss;
}

/**
 * Checks if there are any recent bot installations that need to be notified to the user
 * @param ws WebSocket connection
 * @param userId User ID
 */
async function checkForPendingBotNotifications(ws: WebSocket, userId: string): Promise<void> {
  try {
    console.log(`Checking for pending bot notifications for user ${userId}`);

    // Look for Discord records with recent bot installations
    const recentBotInstallations = await prisma.discord.findMany({
      where: {
        projectId: userId,
        botAdded: true,
        botAddedAt: {
          // Check for bot installations in the last 24 hours
          gte: new Date(Date.now() -  60 * 1000),
        },
      },
    });

    if (recentBotInstallations.length > 0) {
      console.log(
        `Found ${recentBotInstallations.length} recent bot installations for user ${userId}`
      );

      // If we have one or more recent bot installations, send a notification
      for (const installation of recentBotInstallations) {
        // Look up the recent message in chat history
        const sessionId = await getOrCreateChatSession(userId);

        const recentMessages = await prisma.chatMessage.findMany({
          where: {
            sessionId,
            actionTaken: 'BOT_ADDED',
            isFromAgent: true,
            timestamp: {
              gte: new Date(Date.now() - 24 * 60 * 60 * 1000),
            },
          },
          orderBy: {
            timestamp: 'desc',
          },
          take: 1,
        });

        if (recentMessages.length > 0) {
          console.log(
            `Found recent bot installation message for user ${userId}, sending notification`
          );

          // Send the stored message to the client
          ws.send(
            JSON.stringify({
              type: 'message',
              content: recentMessages[0].content,
              action: 'BOT_ADDED',
              isFromAgent: true,
              wasDelayed: true, // Flag to indicate this was a delayed notification
            })
          );

          // Also send the Discord stats update
          ws.send(
            JSON.stringify({
              type: 'discord_bot_installed',
              discord: {
                memberCount: installation.memberCount || 0,
                papersShared: installation.papersShared || 0,
                messagesCount: installation.messagesCount || 0,
                qualityScore: installation.qualityScore || 0,
                verified: true,
                serverName: installation.serverName || 'Your Discord Server',
                botAdded: true,
                serverId: installation.serverId,
              },
            })
          );
        } else {
          // If no message was found but we know a bot was added, create a generic message
          const botAddedMessage = {
            content: `## Discord Bot Successfully Added! 🎉

**Great news!** The verification bot has been successfully added to your Discord server "${installation.serverName || 'Your Discord Server'}".

### Current Stats:
- **Members:** ${installation.memberCount || 0} ${installation.memberCount === 1 ? 'member' : 'members'}
- **Messages:** ${installation.messagesCount || 0}
- **Papers shared:** ${installation.papersShared || 0}

Your Discord server is now fully verified and stats are being tracked automatically.`,
          };

          /*
          ws.send(
            JSON.stringify({
              type: 'message',
              content: botAddedMessage.content,
              action: 'BOT_ADDED',
              isFromAgent: true,
              wasDelayed: true,
            })
          );

          // Also send the Discord stats update
          ws.send(
            JSON.stringify({
              type: 'discord_bot_installed',
              discord: {
                memberCount: installation.memberCount || 0,
                papersShared: installation.papersShared || 0,
                messagesCount: installation.messagesCount || 0,
                qualityScore: installation.qualityScore || 0,
                verified: true,
                serverName: installation.serverName || 'Your Discord Server',
                botAdded: true,
                serverId: installation.serverId,
              },
            })
          );
          */
        }
      }
    } else {
      console.log(`No recent bot installations found for user ${userId}`);
    }
  } catch (error) {
    console.error(`Error checking for pending bot notifications: ${error}`);
  }
}

/**
 * Handle authentication messages
 * @param ws WebSocket connection
 * @param data Authentication data
 */
async function handleAuthentication(ws: WebSocket, data: any): Promise<void> {
  try {
    const { wallet, privyId } = data;

    if (!wallet && !privyId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Wallet address or Privy ID is required',
        })
      );
      return;
    }

    // First try to find user by privyId if provided
    let project = null;
    if (privyId) {
      project = await ProjectService.getByPrivyId(privyId);
    }

    // If not found by privyId, try wallet
    if (!project && wallet) {
      project = await ProjectService.getByWallet(wallet);
    }

    // If still not found, create a new user and project
    if (!project && (wallet || privyId)) {
      // Step 1: Create or find the BioUser
      const userData: {
        privyId: string;
        wallet?: string;
        email?: string;
        fullName?: string;
      } = {
        privyId: privyId || '',
      };
      
      if (wallet) userData.wallet = wallet;
      
      const user = await BioUserService.findOrCreate(userData);

      // Step 2: Create a new project
      const projectData: Prisma.ProjectCreateInput = {
        level: 1,
        members: {
          create: {
            bioUser: {
              connect: { id: user.id }
            },
            role: 'founder'
          }
        }
      };

      project = await ProjectService.create(projectData);
    }

    if (!project) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Failed to authenticate user',
        })
      );
      return;
    }

    // Store active connection
    activeConnections[project.id] = ws;

    // Send success response
    ws.send(
      JSON.stringify({
        type: 'auth_success',
        userId: project.id,
        level: project.level,
      })
    );

    // Check for any bot installations that happened while the user was offline
    //checkForPendingBotNotifications(ws, project.id);

    // Send initial data
    handleInitialConnection(ws, project);
  } catch (error) {
    console.error('Authentication error:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Authentication failed',
      })
    );
  }
}

/**
 * Handle message from user
 * @param ws WebSocket connection
 * @param data Message data
 */
async function handleMessage(ws: WebSocket, data: any): Promise<void> {
  try {
    // Check data validity
    if (!data.content) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Message content is required',
        })
      );
      return;
    }

    // Find user ID from active connections
    let userId = null;
    for (const id in activeConnections) {
      if (activeConnections[id] === ws) {
        userId = id;
        break;
      }
    }

    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Not authenticated',
        })
      );
      return;
    }

    // Get user data
    const project = await ProjectService.getById(userId);
    if (!project) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User not found',
        })
      );
      return;
    }

    // Get or create chat session
    const sessionId = await ChatSessionService.getOrCreateForUser(userId);

    // Save the user message
    await ChatMessageService.saveMessage(sessionId, data.content, false);

    // Check for potential actions (Discord setup, NFT minting, etc.)
    const actionTaken = await handlePotentialActions(ws, userId, project, data.content, '');

    // If no action was taken directly, use the AI to respond
    if (actionTaken.length === 0) {
      await handleAIInteraction(ws, userId, data.content, project.projectName || 'User');
    }
  } catch (error) {
    console.error('Error handling message:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to process message',
      })
    );
  }
}

/**
 * Handle fetching NFTs for a user
 * @param ws WebSocket connection
 * @param userId User ID
 */
async function handleGetNFTs(ws: WebSocket, userId: string): Promise<void> {
  try {
    // Validate the user ID
    if (!userId) {
      // Try to find the user ID from active connections
      for (const id in activeConnections) {
        if (activeConnections[id] === ws) {
          userId = id;
          break;
        }
      }

      if (!userId) {
        ws.send(
          JSON.stringify({
            type: 'error',
            message: 'User ID is required',
          })
        );
        return;
      }
    }

    const nfts = await NFTService.getByProjectId(userId);

    ws.send(
      JSON.stringify({
        type: 'nfts',
        nfts,
      })
    );
  } catch (error) {
    console.error('Error fetching NFTs:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to fetch NFTs',
      })
    );
  }
}

/**
 * Handle checking Discord stats for a user
 * @param ws WebSocket connection
 * @param userId User ID
 */
async function handleCheckDiscordStats(ws: WebSocket, userId: string): Promise<void> {
  try {
    // Validate the user ID
    if (!userId) {
      // Try to find the user ID from active connections
      for (const id in activeConnections) {
        if (activeConnections[id] === ws) {
          userId = id;
          break;
        }
      }

      if (!userId) {
        ws.send(
          JSON.stringify({
            type: 'error',
            message: 'User ID is required',
          })
        );
        return;
      }
    }

    const project = await ProjectService.getById(userId);
    if (!project) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User not found',
        })
      );
      return;
    }

    // Get Discord info if available

    if (!project.Discord) {
      ws.send(
        JSON.stringify({
          type: 'discord_info',
          discord: null,
          message: 'No Discord server connected',
        })
      );
      return;
    }

    // Get the bot installation status
    const botStatus = await getBotInstallationStatus(userId);

    // Send the Discord info
    ws.send(
      JSON.stringify({
        type: 'discord_info',
        discord: {
          serverId: project.Discord.serverId,
          serverName: project.Discord.serverName || 'Your Discord Server',
          memberCount: project.Discord.memberCount,
          messagesCount: project.Discord.messagesCount,
          papersShared: project.Discord.papersShared,
          botAdded: project.Discord.botAdded,
          verified: project.Discord.verified,
          botInstallationUrl: !project.Discord.botAdded ? botStatus.installationLink : null,
        },
      })
    );
  } catch (error) {
    console.error('Error checking Discord stats:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to check Discord stats',
      })
    );
  }
}

/**
 * Handle initial WebSocket connection
 * @param ws WebSocket connection
 * @param project User project
 */
async function handleInitialConnection(ws: WebSocket, project: any): Promise<void> {
  try {
    console.log(`Processing initial connection for project ${project.id}`);

    // Check if this is a first-time user by looking for existing sessions
    const existingSessions = await ChatSessionService.getSessionsByProjectId(project.id);

    const isFirstTimeUser = existingSessions?.length === 0;
    console.log(
      `User ${project.id} is ${isFirstTimeUser ? 'a first-time user' : 'a returning user'} (${existingSessions} existing sessions)`
    );

    // Get or create the chat session
    const sessionId = await ChatSessionService.getOrCreateForUser(project.id);

    // Only send welcome message and auto-mint for first-time users
    if (isFirstTimeUser) {
      console.log(`Sending welcome message for first-time user ${project.id}`);

      // Get user's name and project name for personalized message
      const userName = project.fullName || 'Researcher';
      const projectName = project.projectName || 'your scientific project';

      // Send personalized welcome message
      const welcomeMessage = `Congratulations, ${userName}!
We've just begun the process of bringing ${projectName} to life.
Your journey will be guided by the Portal & your project will level up as you progress along the BioDAO building path.
You can proudly announce that you're participating via the BIO portal as your project takes shape.`;

      await ChatMessageService.saveMessage(sessionId, welcomeMessage, true, 'WELCOME', true);

      // Include Discord info in the welcome message if available
      let discordInfo = null;
      if (project.level >= 2) {
        // Get the Discord info if available
        const discordRecord = await DiscordService.getByProjectId(project.id);

        if (discordRecord) {
          // Add Discord info to send back
          discordInfo = {
            serverId: discordRecord.serverId,
            serverName: discordRecord.serverName,
            memberCount: discordRecord.memberCount,
            messagesCount: discordRecord.messagesCount,
            papersShared: discordRecord.papersShared,
            botAdded: discordRecord.botAdded,
            verified: discordRecord.verified,
          };
        }
      }

      ws.send(
        JSON.stringify({
          type: 'message',
          content: welcomeMessage,
          ...(discordInfo ? { discord: discordInfo } : {}),
        })
      );

      // Check if the user already has NFTs
      const existingNFTs = await NFTService.getByProjectId(project.id);

      const hasIdeaNFT = existingNFTs.some((nft) => nft.type === 'idea');
      const hasVisionNFT = existingNFTs.some((nft) => nft.type === 'vision');

      // Auto-mint both NFTs if not already minted
      if (!hasIdeaNFT) {
        // Wait a moment for better UX (message appears, then minting starts)
        await new Promise((resolve) => setTimeout(resolve, 1500));

        // Prepare minting message
        const mintingMessage = "I'll mint your Idea NFT now based on your project description.";

        await ChatMessageService.saveMessage(
          sessionId,
          mintingMessage,
          true,
          'MINT_IDEA_INTENT',
          true
        );

        ws.send(
          JSON.stringify({
            type: 'message',
            content: mintingMessage,
          })
        );

        // Perform the actual minting
        await handleNftMinting(ws, project.id, 'idea');
      }

      if (!hasVisionNFT) {
        // Wait a moment between minting operations
        await new Promise((resolve) => setTimeout(resolve, 2000));

        // Prepare minting message
        const mintingMessage =
          "Now I'll mint your Vision NFT based on your project vision statement.";

        await ChatMessageService.saveMessage(
          sessionId,
          mintingMessage,
          true,
          'MINT_VISION_INTENT',
          true
        );

        ws.send(
          JSON.stringify({
            type: 'message',
            content: mintingMessage,
          })
        );

        // Perform the actual minting
        await handleNftMinting(ws, project.id, 'vision');
      }
    } else {
      // For returning users, send session ID, chat history, NFTs, level and Discord info

      // Send current level
      ws.send(
        JSON.stringify({
          type: 'level',
          level: project.level,
        })
      );

      // Send NFTs
      const nfts = await NFTService.getByProjectId(project.id);
      ws.send(
        JSON.stringify({
          type: 'nfts',
          nfts,
        })
      );

      // Send chat history
      const messages = await ChatMessageService.getMessagesBySessionId(sessionId);
      ws.send(
        JSON.stringify({
          type: 'chat_history',
          sessionId,
          messages,
        })
      );

      // Send Discord info for returning users if available
      let discordInfo = null;
      if (project.level >= 2) {
        const discordRecord = await DiscordService.getByProjectId(project.id);

        if (discordRecord) {
          // Get the bot installation status
          const botStatus = await getBotInstallationStatus(project.id);

          discordInfo = {
            serverId: discordRecord.serverId,
            serverName: discordRecord.serverName || 'Your Discord Server',
            memberCount: discordRecord.memberCount,
            messagesCount: discordRecord.messagesCount,
            papersShared: discordRecord.papersShared,
            botAdded: discordRecord.botAdded,
            verified: discordRecord.verified,
            botInstallationUrl: !discordRecord.botAdded ? botStatus.installationLink : null,
          };

          ws.send(
            JSON.stringify({
              type: 'discord_info',
              discord: discordInfo,
            })
          );
        }
      }
    }

    // For all users, check level-up conditions
    const projectWithNFTs = await ProjectService.getById(project.id);

    if (projectWithNFTs) {
      await checkAndPerformLevelUp(projectWithNFTs, ws);
    }

    // Send guidance message based on current state for all users
    await sendCoreAgentGuidance(ws, project.id);
  } catch (error) {
    console.error('Error in initial connection handling:', error);

    // Send a fallback message if something went wrong
    ws.send(
      JSON.stringify({
        type: 'error',
        message:
          "Welcome to BioDAO! I'm experiencing some technical difficulties at the moment. Please type a message to continue.",
      })
    );
  }
}

/**
 * Send guidance to the user with level-specific information
 */
async function sendCoreAgentGuidance(
  ws: WebSocket,
  userId: string,
  source: string = 'unknown'
): Promise<void> {
  try {
    // Get the project with relations
    const project = await prisma.project.findUnique({
      where: { id: userId },
      include: {
        NFTs: true,
        Discord: true,
        Twitter: true,
      },
    });

    if (!project) {
      console.error(`Cannot send guidance - project ${userId} not found`);
      return;
    }

    // Get or create chat session
    const sessionId = await ChatSessionService.getOrCreateForUser(userId);

    // Prepare Discord stats if available
    let discordStats = null;
    if (project.Discord) {
      discordStats = {
        serverName: project.Discord.serverName,
        memberCount: project.Discord.memberCount,
        papersShared: project.Discord.papersShared,
        messagesCount: project.Discord.messagesCount,
        qualityScore: project.Discord.qualityScore,
        botAdded: project.Discord.botAdded,
        verified: project.Discord.verified,
      };
    }

    // Get bot installation status and URL if needed for level 2 users without bot
    let botInstallationUrl = null;
    if (project.level === 2 && project.Discord && !project.Discord.botAdded) {
      const botStatus = await getBotInstallationStatus(userId);
      botInstallationUrl = botStatus.installationLink;
    }

    // Calculate user progress
    const progressData = await calculateUserProgress(project);

    // Generate guidance message based on level and progress
    const guidanceMessage = generateNextLevelRequirementsMessage(project.level, project);

    // Send guidance message with Discord info including bot installation URL if available
    ws.send(
      JSON.stringify({
        type: 'message',
        content: guidanceMessage,
        action: 'GUIDANCE',
        level: project.level,
        discord: discordStats
          ? {
              ...discordStats,
              // Include bot installation URL if available and at level 2
              ...(botInstallationUrl ? { botInstallationUrl } : {}),
            }
          : null,
      })
    );

    // Save the guidance message to the chat history
    await ChatMessageService.saveMessage(sessionId, guidanceMessage, true, 'GUIDANCE', true);
  } catch (error) {
    console.error('Error sending CoreAgent guidance:', error);
  }
}

/**
 * Calculate user progress metrics
 */
async function calculateUserProgress(project: any): Promise<any> {
  // Default progress object
  const progress: any = {
    ideaNFT: false,
    visionNFT: false,
    discordCreated: false,
    botAdded: false,
    memberCount: 0,
    papersShared: 0,
    messagesCount: 0,
    qualityScore: 0,
  };

  // Check NFTs status
  if (project.NFTs && project.NFTs.length > 0) {
    for (const nft of project.NFTs) {
      if (nft.type === 'idea') progress.ideaNFT = true;
      if (nft.type === 'vision') progress.visionNFT = true;
    }
  }

  // Check Discord status
  if (project.Discord) {
    progress.discordCreated = true;
    progress.botAdded = project.Discord.botAdded || false;
    progress.memberCount = project.Discord.memberCount || 0;
    progress.papersShared = project.Discord.papersShared || 0;
    progress.messagesCount = project.Discord.messagesCount || 0;
    progress.qualityScore = project.Discord.qualityScore || 0;
  }

  return progress;
}

/**
 * Generate level-specific guidance message
 */
function generateNextLevelRequirementsMessage(currentLevel: number, project: any): string {
  switch (currentLevel) {
    /*
    case 1:
      return [
        `**I'm excited to help you set up your research community!** \nLet me guide you through the process:\n`,
        `**1. Creating a Discord Server:**\n`,
        `- Go to Discord and click the **+** button on the left sidebar`,
        `- Choose **"Create a Server"** and follow the setup wizard`,
        `- You can use this BIO template: https://discord.new/wbyrDkxwyhNp`,
        `- Create channels for research discussions, paper sharing, and community updates\n`,
        `**2. Connecting Your Server:**\n`,
        `- Once your server is set up, share your Discord invite link with me`,
        `- Just paste your Discord invite link here (it will look like discord.gg/123abc)`,
        `- I'll verify the server and then guide you through the next step\n`,
        `**3. Growing Your Community:**\n`,
        `- After verification, you'll need to invite at least **4 members** to reach Level 3`,
        `- Reach out to colleagues, collaborators, and interested researchers\n`,
        `Would you like help creating your Discord server?`,
      ].join('\n');
      */

    case 2:
      const currentMembers = project.Discord?.memberCount || 0;
      const membersNeeded = 4 - currentMembers;
      const botInstalled = project.Discord?.botAdded || false;
      const userDiscordConnected = project.members?.[0]?.bioUser?.discordId ? true : false;
      
      // SCENARIO 1: No Discord entry at all - focus on step 1 only (creating server and sharing invite)
      if (!project.Discord) {
        return [
          `**Let's set up your community on Discord!** \n`,
          `**First Step: Create and Connect Your Discord Server**\n`,
          `- Go to Discord and click the **+** button on the left sidebar`,
          `- Choose **"Create a Server"** and follow the setup wizard`,
          `- You can use this BIO template: https://discord.new/wbyrDkxwyhNp`,
          `- Once created, share your Discord invite link with me here`,
          `- Just paste your Discord invite link (it will look like discord.gg/123abc)\n`,
          `**Optional Resource:** Check out our [Discord Basics Tutorial Video](https://drive.google.com/file/u/1/d/1ntEA39P94KkeZLa2eT2OdrMOFjVbgUbh/preview?pli=1) for helpful tips on setting up an effective server. This is not required for level-up but highly recommended.\n`,
          `**Important:** Share your Discord invite link first, then we'll proceed to the next steps.`,
        ].join('\n');
      }
      // SCENARIO 2: Discord connected but bot not installed - focus on step 2
      else if (!botInstalled) {
        return [
          `**Great progress on setting up your community!** \n`,
          `**Current Status:**\n`,
          `- Discord server connected ✅`,
          `- Verification bot installed ❌`,
          `- Your personal Discord account connected ${userDiscordConnected ? '✅' : '❌'}`,
          `- Current members: ${currentMembers} (need ${membersNeeded > 0 ? `${membersNeeded} more` : 'no more'} to reach 4)\n`,
          `**Next Step: Install Verification Bot**\n`,
          `- I've sent you a link to install our verification bot in a separate message`,
          `- This bot is required to track your community metrics automatically`,
          `- Without the bot, we can't verify your progress toward level-ups\n`,
          `**Optional Resource:** Our [Discord Basics Tutorial Video](https://drive.google.com/file/u/1/d/1ntEA39P94KkeZLa2eT2OdrMOFjVbgUbh/preview?pli=1) includes tips for growing your community and managing your server. Not required for level-up but recommended!\n`,
          `After installing the bot, focus on inviting members to your server to reach at least 4 members.`,
          `Would you like suggestions for growing your community?`,
        ].join('\n');
      } 
      // SCENARIO 3: Bot installed but personal account not connected
      else if (!userDiscordConnected) {
        return [
          `**Great progress on setting up your community!** \n`,
          `**Current Status:**\n`,
          `- Discord server connected ✅`, 
          `- Verification bot installed ✅`,
          `- Your personal Discord account connected ❌`,
          `- Current members: ${currentMembers} (need ${membersNeeded > 0 ? `${membersNeeded} more` : 'no more'} to reach 4)\n`,
          `**Next Step: Connect Your Personal Discord Account**\n`,
          `- Go to ${config.app.url}/settings?tab=connections`,
          `- Click "Connect" next to Discord`,
          `- This connection is required to receive DM notifications when new scientists join`,
          `- The DM system is how scientists will verify their credentials\n`,
          `**Final Step: Grow Your Community**\n`,
          `- Invite researchers and collaborators to join your server`,
          `- Reach the milestone of 4 members to advance to Level 3\n`,
          `**Optional Resource:** Check out our [Discord Basics Tutorial Video](https://drive.google.com/file/u/1/d/1ntEA39P94KkeZLa2eT2OdrMOFjVbgUbh/preview?pli=1) for advanced community growth strategies.\n`,
          `Would you like suggestions for growing your community or tracking your progress?`,
        ].join('\n');
      }
      // SCENARIO 4: Everything set up, just need to grow community
      else {
        return [
          `**Great progress on setting up your community!** \n`,
          `**Current Status:**\n`,
          `- Discord server connected ✅`, 
          `- Verification bot installed ✅`,
          `- Your personal Discord account connected ✅`,
          `- Current members: ${currentMembers} (need ${membersNeeded > 0 ? `${membersNeeded} more` : 'no more'} to reach 4)\n`,
          `**Focus on Growing Your Community:**\n`,
          `- Invite researchers and collaborators to join your server`,
          `- Share interesting scientific content to attract members`,
          `- Host virtual events or discussions`,
          `- Reach the milestone of 4 members to advance to Level 3\n`,
          `**Optional Resource:** Check out our [Discord Basics Tutorial Video](https://drive.google.com/file/u/1/d/1ntEA39P94KkeZLa2eT2OdrMOFjVbgUbh/preview?pli=1) for advanced community growth strategies. This is not required for level-up but may help you succeed.\n`,
          `Would you like suggestions for growing your community or tracking your progress?`,
        ].join('\n');
      }

    case 3:
      const members = project.Discord?.memberCount || 0;
      const papers = project.Discord?.papersShared || 0;
      const messages = project.Discord?.messagesCount || 0;
      return [
        `**You're doing great! Let's get you to Level 4:**\n`,
        `**1. Grow to 10+ Members** *(you need ${Math.max(0, 10 - members)} more)*:\n`,
        `- Share your invite with researchers`,
        `- Host community events`,
        `- Invite participants from aligned communities\n`,
        `**2. Share 25+ Scientific Papers** *(you need ${Math.max(0, 25 - papers)} more)*:\n`,
        `- Share PDFs or links from PubMed, bioRxiv, etc.`,
        `- The bot detects papers shared in your server\n`,
        `**3. Reach 100+ Quality Messages** *(you need ${Math.max(0, 100 - messages)} more)*:\n`,
        `- Encourage rich discussion on research topics`,
        `- Ask thoughtful, open-ended questions`,
        `- The bot tracks and filters quality messages\n`,
        `**Optional Resource:** Our [Discord Basics Tutorial Video](https://drive.google.com/file/u/1/d/1ntEA39P94KkeZLa2eT2OdrMOFjVbgUbh/preview?pli=1) includes advanced tips for managing scientific communities.\n`,
        `Would you like help with outreach or boosting engagement?`,
      ].join('\n');

    case 4:
      // Get Twitter connection status
      const twitterConnected = project.Twitter?.connected || false;
      const introTweetsCount = project.Twitter?.introTweetsCount || 0;
      const tweetsNeeded = Math.max(0, 3 - introTweetsCount);
      
      // If Twitter not connected
      if (!twitterConnected) {
        return [
          `**Congratulations on reaching Level 4!** \nTime to establish your BioDAO's social presence.\n`,
          `**Next Step: Connect Your Twitter Account**\n`,
          `1. Go to ${config.app.url}/settings`,
          `2. Click "Connect" next to Twitter`,
          `3. Authorize the connection with your BioDAO's Twitter account\n`,
          `After connecting, you'll need to publish 3 introductory tweets about your DAO and its mission.\n`,
          `Would you like guidance on what to include in your tweets?`,
        ].join('\n');
      }
      // If Twitter connected but not enough tweets
      else if (introTweetsCount < 3) {
        return [
          `**You're making great progress!** \nYour Twitter account is connected successfully.\n`,
          `**Current Status:**\n`,
          `- Twitter account connected ✅`,
          `- Introductory tweets published: ${introTweetsCount}/3 (need ${tweetsNeeded} more)\n`,
          `**Next Step: Publish Introductory Tweets**\n`,
          `Your tweets should focus on:\n`,
          `1. Your BioDAO's core mission and scientific focus`,
          `2. The specific problems your community aims to solve`,
          `3. An invitation for other researchers to join your community\n`,
          `Use hashtags like #DeSci, #BioDAO, and your specific research field for visibility.\n`,
          `After publishing tweets, Please share the URLs with me here.`,
        ].join('\n');
      }
      // If Twitter connected and enough tweets (shouldn't happen, should level up)
      else {
        return [
          `**You've completed all Level 4 requirements!** \nYou should be advancing to Level 5 shortly.\n`,
          `**Current Status:**\n`,
          `- Twitter account connected ✅`,
          `- Introductory tweets published: ${introTweetsCount}/3 ✅\n`,
          `Your BioDAO has established both a scientific community and social presence. Great work!`,
        ].join('\n');
      }

    case 5:
      // Get Twitter Space and scientist stats
      const scientistCount = (project as any).verifiedScientistCount || 0;
      const hasTwitterSpace = project.Twitter?.twitterSpaceUrl ? true : false;
      const scientistsNeeded = Math.max(0, 10 - scientistCount);

      return [
        `**You're at Level 5: Social Presence** \nNow it's time to build your scientific community.\n`,
        `**Current Status:**\n`,
        `- Discord community established ✅ (${project.Discord?.memberCount || 0} members)`,
        `- Twitter presence established ✅`,
        `- Verified scientists: ${scientistCount}/10 ${scientistCount >= 10 ? '✅' : '❌'}`,
        `- Twitter Space hosted: ${hasTwitterSpace ? '✅' : '❌'}\n`,
        `**Next Steps:**\n`,
        `1. ${scientistCount >= 10 ? '✅' : '❌'} Grow your community to include at least 10 verified scientists or patients${
          scientistsNeeded > 0 ? ` (need ${scientistsNeeded} more)` : ''
        }`,
        `2. ${hasTwitterSpace ? '✅' : '❌'} Host a public Twitter Space to engage your audience${
          !hasTwitterSpace ? ' (share the URL with me after hosting)' : ''
        }\n`,
        `Scientists are verified when they join your Discord and share their scientific profiles through our bot's DM system. The bot will automatically prompt new members to share their credentials.\n\n`,
        `Would you like specific guidance on either of these requirements?`,
      ].join('\n');

    case 6:
      return [
        `🎉 **Congratulations on reaching Level 6!**  \nYou've established a strong scientific community.\n`,
        `**Your BioDAO's Current Status:**\n`,
        `- Scientific NFTs minted ✅`,
        `- Discord community established ✅ (${project.Discord?.memberCount || 0} members)`,
        `- Scientific content shared ✅ (${project.Discord?.papersShared || 0} papers)`,
        `- Active community discussions ✅ (${project.Discord?.messagesCount || 0} messages)`,
        `- Social presence established ✅ (Twitter connected with intro tweets)`,
        `- Scientific community ✅ (${(project as any).verifiedScientistCount || 0} verified scientists)`,
        `- Community engagement ✅ (Hosted Twitter Space)\n`,
        `**Next Steps for Level 7:**\n`,
        `1. Write and publish a visionary blogpost about your DAO's future in 5-10 years`,
        `2. Convert your blogpost into a Twitter thread and share it publicly\n`,
        `This final level focuses on clearly articulating your long-term vision and expanding your public presence. Would you like guidance on writing your visionary blogpost?`,
      ].join('\n');

    case 7:
      return [
        `🎉 **Congratulations on completing all levels of the BioDAO onboarding process!**\n`,
        `**Your BioDAO's Current Status:**\n`,
        `- Scientific NFTs minted ✅`,
        `- Discord community established ✅ (${project.Discord?.memberCount || 0} members)`,
        `- Scientific content shared ✅ (${project.Discord?.papersShared || 0} papers)`,
        `- Active community discussions ✅ (${project.Discord?.messagesCount || 0} messages)`,
        `- Social presence established ✅ (Twitter connected with intro tweets)`,
        `- Scientific community ✅ (${(project as any).verifiedScientistCount || 0} verified scientists)`,
        `- Community engagement ✅ (Hosted Twitter Space)`,
        `- Vision articulated ✅ (Published blogpost and Twitter thread)\n`,
        `**Beyond Onboarding:**\n`,
        `Your BioDAO is now fully established! You have completed all onboarding requirements. The Bio team will be in touch regarding next steps and opportunities within the ecosystem.\n\n`,
        `**Some suggestions for continued growth:**\n`,
        `1. Regularly update your community through Discord and Twitter`,
        `2. Explore funding opportunities through grants and partnerships`,
        `3. Establish regular Twitter Spaces or community calls`,
        `4. Develop a roadmap for your scientific milestones`,
      ].join('\n');

    default:
      return '';
  }
}

/**
 * Check if bot is installed and generate installation link if needed
 * @param projectId Project ID
 * @returns Object with installation status and link
 */
async function getBotInstallationStatus(
  projectId: string
): Promise<{ installed: boolean; installationLink: string | null }> {
  try {
    // Get the Discord server information for this user
    const discordServer = await prisma.discord.findUnique({
      where: { projectId },
    });

    // If no Discord server or bot already added, return appropriate result
    if (!discordServer) {
      return { installed: false, installationLink: null };
    }

    if (discordServer.botAdded) {
      return { installed: true, installationLink: null };
    }

    // Bot not added, generate installation link
    const clientId =
      config.discord.clientId || process.env.DISCORD_CLIENT_ID || '1361285493521907832';
    const permissions = config.discord.permissions || '8';
    const verificationToken =
      discordServer.verificationToken ||
      generateVerificationToken(projectId, discordServer.serverId);

    // Create the installation URL with verification token
    const installationLink = `https://discord.com/api/oauth2/authorize?client_id=${clientId}&permissions=${permissions}&scope=bot&guild_id=${discordServer.serverId}&state=${verificationToken}`;

    // If the server didn't have a verification token, update it now
    if (!discordServer.verificationToken) {
      await prisma.discord.update({
        where: { id: discordServer.id },
        data: { verificationToken },
      });
    }

    return { installed: false, installationLink };
  } catch (error) {
    console.error('Error checking bot installation status:', error);
    return { installed: false, installationLink: null };
  }
}

/**
 * Generate a verification token for Discord
 * @param userId User ID
 * @param serverId Server ID
 * @returns Verification token
 */
function generateVerificationToken(userId: string, serverId: string): string {
  const combinedString = `${userId}:${serverId}:${Date.now()}`;
  return Buffer.from(combinedString).toString('base64');
}

/**
 * Handle AI interaction with the user
 * @param ws WebSocket connection
 * @param userId User ID
 * @param userMessage User message
 * @param userName User name
 */
async function handleAIInteraction(
  ws: WebSocket,
  userId: string,
  userMessage: string,
  userName: string
): Promise<void> {
  try {
    // Get user with Discord info using Prisma directly
    const project = await prisma.project.findUnique({
      where: {
        id: userId,
      },
      include: {
        Discord: true,
        NFTs: true,
      },
    });

    if (!project) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Project not found',
        })
      );
      return;
    }

    // Ensure we have the latest Discord stats if they exist
    let discordStats = project.Discord;

    if (project.level >= 2 && project.Discord) {
      // Refresh Discord stats from API if possible
      try {
        const { inviteCode } = extractDiscordInfo(project.Discord.inviteLink || '');
        if (inviteCode) {
          const serverInfo = await fetchDiscordServerInfo(inviteCode);
          if (serverInfo.approximateMemberCount) {
            // Update member count in database
            await prisma.discord.update({
              where: { id: project.Discord.id },
              data: {
                memberCount: serverInfo.approximateMemberCount,
                updatedAt: new Date(),
              },
            });

            // Update local copy for this request
            discordStats = {
              ...project.Discord,
              memberCount: serverInfo.approximateMemberCount,
            };

            console.log(
              `Updated Discord stats for user ${userId}: ${discordStats.memberCount} members`
            );
          }
        }
      } catch (error) {
        console.error('Error refreshing Discord stats:', error);
        // Continue with existing data if refresh fails
      }
    }

    // Check if user is at level 2 but has no Discord set up yet
    if (project.level === 2 && !discordStats) {
      console.log(`User ${userId} is at level 2 but has no Discord server set up yet`);
    }

    // Get or create a chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Save the user's message (this is already done in handleMessage, but adding here for completeness)
    await saveChatMessage(sessionId, userMessage, false);

    // Check bot installation status if applicable
    const botStatus = await getBotInstallationStatus(userId);

    // Add bot installation info to message if needed
    let aiContext = userMessage;
    if (
      botStatus.installationLink &&
      (aiContext.toLowerCase().includes('bot') ||
        aiContext.toLowerCase().includes('discord') ||
        aiContext.toLowerCase().includes('verify') ||
        aiContext.toLowerCase().includes('verification'))
    ) {
      // Append installation link info to the AI context
      aiContext += `\n\nNote: When referring to the bot installation link, use the placeholder [BOT_INSTALLATION_LINK] which will be automatically replaced with the actual link.`;
    }

    // Send typing indicator
    ws.send(
      JSON.stringify({
        type: 'agent_typing',
        isTyping: true,
      })
    );

    // Process message with AI to get the response, providing the most up-to-date real Discord stats
    const aiResponse = await processMessage(
      userId,
      aiContext, // Use the context with potential bot info
      project.level,
      discordStats,
      botStatus.installationLink || undefined // Convert null to undefined for type safety
    );

    // We don't need to replace placeholders or add the link separately anymore
    // since we're passing it directly to the AI model
    let enhancedResponse = aiResponse;

    // Save the AI's enhanced response
    await saveChatMessage(sessionId, enhancedResponse, true);

    // Send AI response to the client
    ws.send(
      JSON.stringify({
        type: 'message',
        content: enhancedResponse,
        isFromAgent: true,
      })
    );

    // Turn off typing indicator
    ws.send(
      JSON.stringify({
        type: 'agent_typing',
        isTyping: false,
      })
    );

    // Process potential actions based on the conversation
    const actionsDetected = await handlePotentialActions(
      ws,
      userId,
      { ...project, Discord: discordStats },
      userMessage,
      enhancedResponse
    );

    // If actions were taken, update the AI message to record them
    if (actionsDetected.length > 0) {
      console.log(`Actions detected and handled for user ${userId}:`, actionsDetected);

      // Save each action as a system message in the chat history
      for (const action of actionsDetected) {
        await saveChatMessage(
          sessionId,
          `System action: ${action.action}`,
          true,
          action.action,
          action.success
        );
      }
    }

    // Check for level-up conditions after AI has responded
    await checkLevelUpConditions(userId, project.level, discordStats, ws);
  } catch (error) {
    console.error('Error handling AI interaction:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to get AI response',
      })
    );

    // Turn off typing indicator
    ws.send(
      JSON.stringify({
        type: 'agent_typing',
        isTyping: false,
      })
    );
  }
}

/**
 * Handle potential actions based on user message and AI response
 * @param ws WebSocket connection
 * @param userId User ID
 * @param project Project data
 * @param userMessage User message
 * @param aiResponse AI response
 * @returns Array of actions taken
 */
async function handlePotentialActions(
  ws: WebSocket,
  userId: string,
  project: any,
  userMessage: string,
  aiResponse: string
): Promise<Array<{ action: string; success: boolean }>> {
  const actions: Array<{ action: string; success: boolean }> = [];

  try {
    // Check for Discord invite links if user is at level 2 or higher
    if (project.level >= 2) {
      const { inviteLink } = extractDiscordInfo(userMessage);

      if (inviteLink) {
        console.log(`Found Discord invite link in message: ${inviteLink}`);

        // Process the Discord setup
        const success = await handleDiscordSetup(ws, userId, { content: userMessage });

        if (success) {
          actions.push({ action: 'discord_setup', success: true });
          console.log(`Discord setup processed successfully for user ${userId}`);
        } else {
          console.log(`Discord setup failed for user ${userId}`);
        }
      }
    }

    // Check for NFT minting request in user message
    const isIdeaNFTRequest =
      userMessage.toLowerCase().includes('idea nft') ||
      userMessage.toLowerCase().includes('mint idea');

    const isVisionNFTRequest =
      userMessage.toLowerCase().includes('vision nft') ||
      userMessage.toLowerCase().includes('mint vision') ||
      userMessage.toLowerCase().includes('mint hypothesis');

    if (isIdeaNFTRequest && project.level === 1) {
      const success = await handleNftMinting(ws, userId, 'idea');
      actions.push({ action: 'mint_idea_nft', success });
    }

    if (isVisionNFTRequest && project.level === 1) {
      const success = await handleNftMinting(ws, userId, 'vision');
      actions.push({ action: 'mint_vision_nft', success });
    }

    // Check for tweet URLs if user is at level 4
    if (project.level === 4) {
      // Check if user message contains tweet URLs
      const tweetUrls = extractTweetUrls(userMessage);
      
      if (tweetUrls.length > 0) {
        console.log(`Found ${tweetUrls.length} tweet URLs in message: ${tweetUrls.join(', ')}`);
        
        // Process the tweet verification
        const success = await handleTweetVerification(ws, userId, tweetUrls);
        
        if (success) {
          actions.push({ action: 'verify_tweets', success: true });
          console.log(`Tweet verification processed successfully for user ${userId}`);
        } else {
          console.log(`Tweet verification failed for user ${userId}`);
        }
      } 
      // Check if user is explicitly asking to verify their tweets
      else if (
        userMessage.toLowerCase().includes('verify my tweets') ||
        userMessage.toLowerCase().includes('check my tweets') ||
        userMessage.toLowerCase().includes('verify tweets') ||
        userMessage.toLowerCase().includes('submitted tweets') ||
        userMessage.toLowerCase().includes('published tweets')
      ) {
        // Process general tweet verification (checks recent tweets)
        try {
          await handleVerifyTwitterTweets(ws, userId);
          actions.push({ action: 'verify_recent_tweets', success: true });
          console.log(`Recent tweet verification processed for user ${userId}`);
        } catch (error) {
          console.error('Error verifying recent tweets:', error);
          actions.push({ action: 'verify_recent_tweets', success: false });
        }
      }
    }

    // Check for blogpost URL and Twitter thread URL if user is at level 6
    if (project.level === 6) {
      // Extract blogpost URLs (general URLs that aren't Twitter)
      const blogpostUrls = extractBlogpostUrls(userMessage);
      
      if (blogpostUrls.length > 0) {
        console.log(`Found blogpost URL in message: ${blogpostUrls[0]}`);
        
        // Process the first blogpost URL (to avoid multiple processing)
        try {
          await handleVerifyBlogpost(ws, userId, blogpostUrls[0]);
          actions.push({ action: 'verify_blogpost', success: true });
          console.log(`Blogpost verification processed successfully for user ${userId}`);
        } catch (error) {
          console.error('Error verifying blogpost:', error);
          actions.push({ action: 'verify_blogpost', success: false });
        }
      }
      
      // Extract Twitter thread URLs
      const twitterThreadUrls = extractTwitterThreadUrls(userMessage);
      
      if (twitterThreadUrls.length > 0) {
        console.log(`Found Twitter thread URL in message: ${twitterThreadUrls[0]}`);
        
        // Process the first Twitter thread URL (to avoid multiple processing)
        try {
          await handleVerifyTwitterThread(ws, userId, twitterThreadUrls[0]);
          actions.push({ action: 'verify_twitter_thread', success: true });
          console.log(`Twitter thread verification processed successfully for user ${userId}`);
        } catch (error) {
          console.error('Error verifying Twitter thread:', error);
          actions.push({ action: 'verify_twitter_thread', success: false });
        }
      }
    }

    // More potential actions can be added here in the future

    return actions;
  } catch (error) {
    console.error('Error processing potential actions:', error);
    return actions;
  }
}

/**
 * Extract tweet URLs from a message
 * @param message User message
 * @returns Array of tweet URLs found in the message
 */
function extractTweetUrls(message: string): string[] {
  const tweetUrls: string[] = [];
  
  // Regular expressions to match Twitter URLs
  const twitterRegex = /https?:\/\/(www\.)?(twitter|x)\.com\/[a-zA-Z0-9_]+\/status\/\d+/g;
  
  // Find all matches
  const matches = message.match(twitterRegex);
  
  if (matches) {
    // Filter out duplicates
    const uniqueUrls = [...new Set(matches)];
    tweetUrls.push(...uniqueUrls);
  }
  
  return tweetUrls;
}

/**
 * Handle tweet verification from chat messages
 * @param ws WebSocket connection
 * @param userId User ID
 * @param tweetUrls Array of tweet URLs to verify
 * @returns Success status
 */
async function handleTweetVerification(
  ws: WebSocket,
  userId: string,
  tweetUrls: string[]
): Promise<boolean> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return false;
    }

    if (!tweetUrls || tweetUrls.length === 0) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'No tweet URLs found in your message',
        })
      );
      return false;
    }

    // Get or create chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Save chat message acknowledging tweet submissions
    await saveChatMessage(
      sessionId,
      `I've detected ${tweetUrls.length} tweet URLs in your message. Verifying now...`,
      true,
      'tweets_detected',
      true
    );

    // Update user with a status message
    ws.send(
      JSON.stringify({
        type: 'message',
        content: `I've detected ${tweetUrls.length} tweet URLs. Analyzing and verifying these tweets...`,
        isFromAgent: true,
      })
    );

    // Process the tweet verification using the TwitterService
    await handleSubmitTwitterTweets(ws, userId, tweetUrls);
    
    return true;
  } catch (error) {
    console.error('Error verifying tweets:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to verify tweets',
      })
    );
    return false;
  }
}

/**
 * Handle Discord setup
 * @param ws WebSocket connection
 * @param userId User ID
 * @param data Message data
 * @returns Success status
 */
async function handleDiscordSetup(
  ws: WebSocket,
  userId: string,
  data: { content: string }
): Promise<boolean> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return false;
    }

    const { content } = data;
    const discordInfo = extractDiscordInfo(content);
    const { inviteLink, inviteCode } = discordInfo;

    if (!inviteLink && !inviteCode) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'No Discord invite link found. Please provide a valid Discord invite link.',
        })
      );
      return false;
    }

    console.log(
      `Processing Discord setup for user ${userId} with invite: ${inviteLink || inviteCode}`
    );

    // Process the invite to get server info
    let serverInfo;
    if (inviteCode) {
      serverInfo = await fetchDiscordServerInfo(inviteCode);
    }

    if (!serverInfo || !serverInfo.serverId) {
      const errorMsg =
        serverInfo?.error ||
        'Failed to retrieve Discord server information. The invite link may be invalid or expired.';
      ws.send(
        JSON.stringify({
          type: 'error',
          message: errorMsg,
        })
      );
      return false;
    }

    const finalServerId = serverInfo.serverId;
    const fullInviteLink = inviteLink || `https://discord.gg/${inviteCode}`;
    const serverName = serverInfo.name;
    const serverIcon = serverInfo.icon;
    const memberCount = serverInfo.approximateMemberCount || 0;

    // Generate verification token for this server
    const verificationToken = generateVerificationToken(userId, finalServerId);

    console.log(
      `Discord server info retrieved - ID: ${finalServerId}, Name: ${serverName}, Members: ${memberCount}`
    );

    // Check if this Discord server is already associated with this user
    const existingDiscord = await prisma.discord.findFirst({
      where: {
        serverId: finalServerId,
        projectId: userId,
      },
    });

    if (existingDiscord) {
      console.log(`Updating existing Discord record for user ${userId}:`, existingDiscord);

      // Update the record
      await prisma.discord.update({
        where: { id: existingDiscord.id },
        data: {
          memberCount,
          inviteLink: fullInviteLink,
          serverName,
          serverIcon,
          // Don't override botAdded status
          verificationToken,
          updatedAt: new Date(),
        },
      });
    } else {
      // Create a new record
      const updatedDiscord = await prisma.discord.create({
        data: {
          serverId: finalServerId,
          inviteLink: fullInviteLink,
          projectId: userId,
          memberCount,
          papersShared: 0,
          messagesCount: 0,
          qualityScore: 0,
          serverName,
          serverIcon,
          verificationToken,
          botAdded: false,
        },
      });
      console.log(`Created Discord record for user ${userId}:`, updatedDiscord);
    }

    // Generate bot installation URL with verification token
    // This token will be passed back when the bot is added, confirming the proper server
    const botInstallationUrl = `https://discord.com/api/oauth2/authorize?client_id=${process.env.DISCORD_CLIENT_ID}&permissions=8&scope=bot&guild_id=${finalServerId}&state=${verificationToken}`;

    // Format server display info
    const serverDisplayName = serverName || `Server ID: ${finalServerId.substring(0, 10)}...`;

    // Get or create chat session
    const chatSession = await getOrCreateChatSession(userId);

    // Save chat message about Discord setup
    await saveChatMessage(
      chatSession,
      `Discord server "${serverDisplayName}" verified successfully with ${memberCount} members`,
      false,
      'discord_setup_initiated',
      true
    );

    // STEP 1: Send confirmation message about successful server connection
    const serverConnectedMessage = {
      content: `## Discord Server Connected: "${serverDisplayName}" 🎉

**Current Members:** ${memberCount} ${memberCount === 1 ? 'member' : 'members'}

Great job! I've successfully connected to your Discord server. Now we need to complete one more important step.`
    };

    await saveChatMessage(chatSession, serverConnectedMessage, true, 'discord_server_connected', true);

    // Send success message with Discord info
    ws.send(
      JSON.stringify({
        type: 'discord_setup_success',
        message: 'Discord server verified and registered successfully.',
        discord: {
          serverId: finalServerId,
          inviteLink: fullInviteLink,
          memberCount,
          serverName: serverDisplayName,
          serverIcon,
          papersShared: 0,
          messagesCount: 0,
          qualityScore: 0,
          botAdded: false,
          verified: false,
          botInstallationUrl: botInstallationUrl,
        },
        pendingVerification: true,
      })
    );

    // Also send agent message as a regular chat message to ensure it shows up in the chat
    ws.send(
      JSON.stringify({
        type: 'message',
        content: serverConnectedMessage.content,
        isFromAgent: true,
        action: 'discord_server_connected',
      })
    );

    // STEP 2: Send a separate message about the bot installation
    setTimeout(async () => {
      const botInstallMessage = {
        content: `## Next Step: Install Verification Bot

To track your community's progress, please install our verification bot.

**[Click here to install the BioDAO verification bot](${botInstallationUrl})**

This verification bot helps:
- Track member count automatically
- Count messages and scientific papers shared
- Verify your progress toward level advancement

Once installed, your Discord stats will be automatically tracked for level progression.`
      };

      await saveChatMessage(chatSession, botInstallMessage, true, 'bot_install_prompt', true);

      ws.send(
        JSON.stringify({
          type: 'message',
          content: botInstallMessage.content,
          isFromAgent: true,
          action: 'bot_install_prompt',
        })
      );
    }, 2000); // Send the bot installation prompt 2 seconds after the server connection confirmation

    // Trigger an AI interaction to provide contextual guidance
    try {
      const project = await ProjectService.getById(userId);
      if (project) {
        // Send typing indicator
        ws.send(
          JSON.stringify({
            type: 'agent_typing',
            isTyping: true,
          })
        );

        // Get AI response specific to Discord setup
        const setupPrompt = `The user has just connected their Discord server "${serverDisplayName}" with ${memberCount} members. I've already sent them the verification bot installation instructions in a separate message. Focus on the next steps after they install the bot - growing their community to at least 4 members.`;

        // Process the message with AI
        const aiResponse = await processMessage(
          userId,
          setupPrompt,
          project.level,
          {
            serverId: finalServerId,
            serverName: serverDisplayName,
            memberCount,
            botAdded: false,
            verified: false,
            botInstallationUrl: botInstallationUrl,
            papersShared: 0,
            messagesCount: 0,
          },
          botInstallationUrl || undefined
        );

        // Turn off typing indicator
        ws.send(
          JSON.stringify({
            type: 'agent_typing',
            isTyping: false,
          })
        );
      }
    } catch (error) {
      console.error('Error getting AI guidance after Discord setup:', error);
    }
    const latestProject = await ProjectService.getById(userId);
    // Check and perform level up
    await checkAndPerformLevelUp(latestProject, ws);

    return true;
  } catch (error) {
    console.error('Error setting up Discord:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message:
          'Failed to register Discord server. Please try again with a valid Discord invite link.',
      })
    );
    return false;
  }
}

/**
 * Handle NFT minting
 * @param ws WebSocket connection
 * @param userId User ID
 * @param nftType NFT type ('idea' or 'vision')
 * @returns Success status
 */
async function handleNftMinting(ws: WebSocket, userId: string, nftType: string): Promise<boolean> {
  try {
    // Check if user already has this NFT type
    const existingNFTs = await NFTService.getByProjectId(userId);
    const hasNFT = existingNFTs.some((nft: any) => nft.type === nftType);

    if (hasNFT) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: `You already have a ${nftType} NFT`,
        })
      );
      return false;
    }

    // Get the project
    const project = await ProjectService.getById(userId);

    if (!project) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Project not found',
        })
      );
      return false;
    }

    // Send minting in progress notification
    ws.send(
      JSON.stringify({
        type: 'nft_minting',
        nftType,
        status: 'started',
      })
    );

    // Mint the NFT
    let transactionHash;
    try {
      if (nftType === 'idea') {
        transactionHash = await mintIdeaNft(project.members[0].bioUser.wallet as any);
      } else if (nftType === 'vision' || nftType === 'hypothesis') {
        transactionHash = await mintVisionNft(project.members[0].bioUser.wallet as any);
      } else {
        ws.send(
          JSON.stringify({
            type: 'error',
            message: `Invalid NFT type: ${nftType}`,
          })
        );
        return false;
      }
    } catch (error) {
      console.error('Error minting NFT:', error);
      ws.send(
        JSON.stringify({
          type: 'nft_minting',
          nftType,
          status: 'failed',
          error: 'Failed to mint NFT',
        })
      );
      return false;
    }

    // Create NFT record
    const nft = await NFTService.create({
      type: nftType,
      projectId: userId,
      transactionHash,
    });

    // Send minting success notification
    ws.send(
      JSON.stringify({
        type: 'nft_minting',
        nftType,
        status: 'success',
        nftId: nft.id,
      })
    );

    // Generate NFT image in background
    generateNftImageInBackground(userId, nft.id, nftType, project, ws);

    // Check if user has both NFTs and should level up
    await checkLevelUpConditions(userId, project.level, null, ws);

    // In handleNftMinting, after NFT image generation and before calling checkAndPerformLevelUp
    const latestProject = await ProjectService.getById(userId);
    if (latestProject) {
      await checkAndPerformLevelUp(latestProject, ws);
    }

    return true;
  } catch (error) {
    console.error('Error handling NFT minting:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to mint NFT',
      })
    );
    return false;
  }
}

/**
 * Generate NFT image in background
 * @param userId User ID
 * @param nftId NFT ID
 * @param nftType NFT type
 * @param project Project data
 * @param ws WebSocket connection
 */
async function generateNftImageInBackground(
  userId: string,
  nftId: string,
  nftType: string,
  project: any,
  ws: WebSocket
): Promise<void> {
  try {
    ws.send(
      JSON.stringify({
        type: 'nft_image_generation',
        nftId,
        status: 'started',
      })
    );

    let imageUrl;
    if (nftType === 'idea') {
      imageUrl = await generateIdeaNFTImage(userId, project.projectDescription || '');
    } else {
      imageUrl = await generateVisionNFTImage(userId, project.projectVision || '');
    }

    // Update NFT with image URL
    await NFTService.update(nftId, { imageUrl });

    // Send the updated NFT info
    ws.send(
      JSON.stringify({
        type: 'nft_image_generation',
        nftId,
        status: 'success',
        imageUrl,
      })
    );

    // Send updated NFTs list
    const nfts = await NFTService.getByProjectId(userId);
    ws.send(
      JSON.stringify({
        type: 'nfts',
        nfts,
      })
    );
  } catch (error) {
    console.error('Error generating NFT image:', error);
    ws.send(
      JSON.stringify({
        type: 'nft_image_generation',
        nftId,
        status: 'failed',
        error: 'Failed to generate NFT image',
      })
    );
  }
}

/**
 * Check level-up conditions
 * @param userId User ID
 * @param currentLevel Current level
 * @param discordStats Discord stats
 * @param ws WebSocket connection
 */
async function checkLevelUpConditions(
  userId: string,
  currentLevel: number,
  discordStats: any,
  ws: WebSocket
): Promise<void> {
  try {
    // Get complete project data for level-up checks
    const project = await prisma.project.findUnique({
      where: { id: userId },
      include: {
        Discord: true,
        NFTs: true,
        members: { include: { bioUser: true } }, // Added this line
        Twitter: true, // Include Twitter data for Level 5 checks
      },
    });

    if (!project) {
      console.error(`Project not found for user ID: ${userId}`);
      return;
    }

    // Skip if user is already at max level
    if (project.level >= 6) return;
    

    // Level 2 to Level 3: Need 4+ Discord members and verified status
    if (currentLevel === 2 && discordStats && discordStats.verified && discordStats.memberCount >= 4) {
      // Define the new level
      const newLevel = 3;

      // Check if we've recently sent this level-up notification
      if (wasLevelUpRecentlySent(userId, newLevel)) {
        console.log(`Skipping duplicate level ${newLevel} notification for user ${userId} (sent recently)`);
        return;
      }

      // Update to level 3
      await prisma.project.update({
        where: { id: userId },
        data: { level: newLevel },
      });

      console.log(`User ${userId} automatically progressed to Level ${newLevel}`);

      // Get session for chat history
      const sessionId = await getOrCreateChatSession(userId);

      // Create level-up message
      const levelUpMessage = `## Level ${newLevel} Unlocked! 🎉

**Congratulations!** Your BioDAO community now has **${discordStats.memberCount} members**, which means you've advanced to **Level ${newLevel}!**

To advance to Level 4, you'll need to:
1. **Grow your community to 10+ members**
2. **Share 25+ scientific papers** in your Discord
3. **Reach 100+ quality messages**

I'll help you track these metrics and provide strategies to achieve them.`;

      // Save level-up message to chat
      await saveChatMessage(sessionId, levelUpMessage, true, 'LEVEL_UP', true);

      // Send level-up message to WebSocket
      ws.send(
        JSON.stringify({
          type: 'level_up',
          previousLevel: currentLevel,
          newLevel: newLevel,
          message: levelUpMessage,
          nextLevelRequirements: [
            'Grow your community to 10+ Discord members',
            'Share at least 25 scientific papers in your server',
            'Reach 100+ quality messages in your community',
          ],
        })
      );

      // Also send as a regular message for clients that don't handle level_up events
      ws.send(
        JSON.stringify({
          type: 'message',
          content: levelUpMessage,
          isFromAgent: true,
          action: 'LEVEL_UP',
        })
      );

      // Record that we've sent this level-up notification
      recordLevelUpSent(userId, newLevel);

      // Send email notifications to all project members
      try {
        await sendLevelUpEmailsToAllMembers(project, newLevel);
      } catch (error) {
        console.error(`Error sending level up emails for project ${userId}:`, error);
      }
    }
    // Level 3 to Level 4: Need 10+ members, 25+ papers, 100+ messages
    else if (
      currentLevel === 3 &&
      discordStats &&
      discordStats.verified &&
      discordStats.memberCount >= 10 &&
      discordStats.papersShared >= 25 &&
      discordStats.messagesCount >= 100
    ) {
      // Define the new level
      const newLevel = 4;

      // Check if we've recently sent this level-up notification
      if (wasLevelUpRecentlySent(userId, newLevel)) {
        console.log(`Skipping duplicate level ${newLevel} notification for user ${userId} (sent recently)`);
        return;
      }

      // Update to level 4
      await prisma.project.update({
        where: { id: userId },
        data: { level: newLevel },
      });

      console.log(`User ${userId} automatically progressed to Level ${newLevel}`);

      // Get session for chat history
      const sessionId = await getOrCreateChatSession(userId);

      // Create level-up message
      const levelUpMessage = `## Level ${newLevel} Unlocked! 🎉

**Congratulations!** Your BioDAO community has reached critical mass with:
- **${discordStats.memberCount} members**
- **${discordStats.papersShared} scientific papers shared**
- **${discordStats.messagesCount} messages** in your server

You've advanced to **Level ${newLevel}!**

To advance to Level 5, you need to establish your social presence:
1. **Connect DAO Twitter account** via the settings page (${config.app.url}/settings?tab=connections)
2. **Publish 3 introductory tweets** about your DAO and its mission`;

      // Save level-up message to chat
      await saveChatMessage(sessionId, levelUpMessage, true, 'LEVEL_UP', true);

      // Send level-up message to WebSocket
      ws.send(
        JSON.stringify({
          type: 'level_up',
          previousLevel: currentLevel,
          newLevel: newLevel,
          message: levelUpMessage,
          nextLevelRequirements: [
            'Connect your Twitter account',
            'Publish 3 introductory tweets about your DAO',
          ],
        })
      );

      // Also send as a regular message for clients that don't handle level_up events
      ws.send(
        JSON.stringify({
          type: 'message',
          content: levelUpMessage,
          isFromAgent: true,
          action: 'LEVEL_UP',
        })
      );

      // Record that we've sent this level-up notification
      recordLevelUpSent(userId, newLevel);

      // Send email notifications
      if (project.members[0].bioUser.email) {
        try {
          // Send level up email
          await sendLevelUpEmail(project.members[0].bioUser.email, newLevel);
          console.log(`Sent level ${newLevel} email to ${project.members[0].bioUser.email}`);
        } catch (error) {
          console.error(`Error sending emails to ${project.members[0].bioUser.email}:`, error);
        }
      }
    }
    // Level 4 to Level 5: Twitter connected and 3+ intro tweets
    else if (
      currentLevel === 4 &&
      project.Twitter &&
      project.Twitter.connected &&
      project.Twitter.introTweetsCount >= 3
    ) {
      // Define the new level
      const newLevel = 5;

      // Check if we've recently sent this level-up notification
      if (wasLevelUpRecentlySent(userId, newLevel)) {
        console.log(`Skipping duplicate level ${newLevel} notification for user ${userId} (sent recently)`);
        return;
      }

      // Update to level 5
      await prisma.project.update({
        where: { id: userId },
        data: { level: newLevel },
      });

      console.log(`User ${userId} automatically progressed to Level ${newLevel}`);

      // Get session for chat history
      const sessionId = await getOrCreateChatSession(userId);

      // Create level-up message
      const levelUpMessage = `## Level ${newLevel} Unlocked! 🎉

**Congratulations!** You've completed all levels of the BioDAO onboarding process by establishing your social presence with:
- **Twitter account connected**
- **${project.Twitter.introTweetsCount} introductory tweets published**

You've reached the highest level of the BioDAO onboarding journey! Your community now has:
- A strong Discord community with ${discordStats.memberCount} members
- Scientific credibility with ${discordStats.papersShared} papers shared
- Active discussion with ${discordStats.messagesCount} messages
- Social media presence with connected Twitter

Next steps:
1. Continue growing your community on Discord and Twitter
2. Explore funding opportunities through the BioDAO ecosystem
3. Develop partnerships with other DeSci projects
4. Check out the dashboard for additional resources`;

      // Save level-up message to chat
      await saveChatMessage(sessionId, levelUpMessage, true, 'LEVEL_UP', true);

      // Send level-up message to WebSocket
      ws.send(
        JSON.stringify({
          type: 'level_up',
          previousLevel: currentLevel,
          newLevel: newLevel,
          message: levelUpMessage,
          nextLevelRequirements: [
            'All requirements completed - congratulations!',
            'You have reached the highest level in the BioDAO onboarding process',
          ],
        })
      );

      // Also send as a regular message for clients that don't handle level_up events
      ws.send(
        JSON.stringify({
          type: 'message',
          content: levelUpMessage,
          isFromAgent: true,
          action: 'LEVEL_UP',
        })
      );

      // Record that we've sent this level-up notification
      recordLevelUpSent(userId, newLevel);

      // Send email notifications
      if (project.members[0].bioUser.email) {
        try {
          // Send level up email
          await sendLevelUpEmail(project.members[0].bioUser.email, newLevel);
          console.log(`Sent final level ${newLevel} email to ${project.members[0].bioUser.email}`);
        } catch (error) {
          console.error(`Error sending emails to ${project.members[0].bioUser.email}:`, error);
        }
      }
    }
    // Level 5 to Level 6: Verified scientists count
    else if (
      currentLevel === 5 &&
      (project as any).verifiedScientistCount >= 10
    ) {
      // Define the new level
      const newLevel = 6;

      // Check if we've recently sent this level-up notification
      if (wasLevelUpRecentlySent(userId, newLevel)) {
        console.log(`Skipping duplicate level ${newLevel} notification for user ${userId} (sent recently)`);
        return;
      }

      // Update to level 6
      await prisma.project.update({
        where: { id: userId },
        data: { level: newLevel },
      });

      console.log(`User ${userId} automatically progressed to Level ${newLevel}`);

      // Get session for chat history
      const sessionId = await getOrCreateChatSession(userId);

      // Create level-up message
      const levelUpMessage = `🎉 **Congratulations on reaching Level 6!**  \nYou've successfully built a community with 10+ verified scientists or patients.\n\nThe Bio team will reach out to discuss next steps for your scientific community.`;

      // Save level-up message to chat
      await saveChatMessage(sessionId, levelUpMessage, true, 'LEVEL_UP', true);

      // Send level-up message to WebSocket
      ws.send(
        JSON.stringify({
          type: 'level_up',
          previousLevel: currentLevel,
          newLevel: newLevel,
          message: levelUpMessage,
          nextLevelRequirements: [
            'All requirements completed - congratulations!',
            'You have reached the highest level in the BioDAO onboarding process',
          ],
        })
      );

      // Also send as a regular message for clients that don't handle level_up events
      ws.send(
        JSON.stringify({
          type: 'message',
          content: levelUpMessage,
          isFromAgent: true,
          action: 'LEVEL_UP',
        })
      );

      // Record that we've sent this level-up notification
      recordLevelUpSent(userId, newLevel);

      // Send email notifications
      if (project.members[0].bioUser.email) {
        try {
          // Send level up email
          await sendLevelUpEmail(project.members[0].bioUser.email, newLevel);
          console.log(`Sent final level ${newLevel} email to ${project.members[0].bioUser.email}`);
        } catch (error) {
          console.error(`Error sending emails to ${project.members[0].bioUser.email}:`, error);
        }
      }
    }
    // Level 6 to Level 7: Blogpost and Twitter thread
    else if (
      currentLevel === 6 &&
      project.Twitter &&
      (project.Twitter as any).blogpostUrl &&
      (project.Twitter as any).twitterThreadUrl
    ) {
      // Check if we've recently sent this level-up notification
      if (wasLevelUpRecentlySent(project.id, 7)) {
        console.log(`Skipping duplicate level 7 notification for user ${project.id} (sent recently)`);
        return;
      }
      
      const newLevel = 7;
      const shouldLevelUp = true;
      const levelUpMessage = `🎉 **Congratulations on reaching Level 7!**\n\nYou've successfully articulated your BioDAO's vision through both a comprehensive blogpost and an engaging Twitter thread. This is the final level of the BioDAO onboarding process.\n\nTo complete the entire onboarding process, you need to:\n- Record a welcome Loom video for new members\n- Share the vision of your DAO\n- Post it on Discord and share the link with me`;
      
      // Code to perform the level up would go here, similar to other level transitions
      // Update the level in the database
      await prisma.project.update({
        where: { id: project.id },
        data: { level: newLevel },
      });

      // Get or create chat session for this user
      const sessionId = await ChatSessionService.getOrCreateForUser(project.id);
      
      // Save the level up message to chat history
      await ChatMessageService.saveMessage(sessionId, levelUpMessage, true, 'LEVEL_UP', true);

      // Send a message about leveling up
      if (ws) {
        ws.send(
          JSON.stringify({
            type: 'message',
            content: levelUpMessage,
            isFromAgent: true,
            action: 'level_up',
          })
        );
        
        // Also send the level_up event
        ws.send(
          JSON.stringify({
            type: 'level_up',
            previousLevel: currentLevel,
            newLevel: newLevel,
          })
        );
      }

      // Record that we've sent this level-up notification
      recordLevelUpSent(project.id, newLevel);
    } else {
      const missingReqs = [];
      if (!project.Twitter) {
        missingReqs.push('Twitter connection required');
      } else {
        if (!(project.Twitter as any).blogpostUrl) missingReqs.push('visionary blogpost');
        if (!(project.Twitter as any).twitterThreadUrl) missingReqs.push('Twitter thread');
      }
      console.log(
        `Project ${project.id} doesn't meet level 7 requirements: ${missingReqs.join(', ')} not verified`
      );
    }
  } catch (error) {
    console.error('Error checking level-up conditions:', error);
  }
}

/**
 * Check if the project meets criteria for level up and perform level up if needed
 * @param project The project data with NFTs included
 * @param ws WebSocket connection to notify the client
 */
async function checkAndPerformLevelUp(project: any, ws: WebSocket): Promise<void> {
  try {
    if (!project || typeof project.level === 'undefined') {
      console.warn('checkAndPerformLevelUp: project is undefined or missing level property:', project);
      return;
    }
    // Get current level
    const currentLevel = project.level || 1;
    let newLevel = currentLevel;
    let shouldLevelUp = false;
    let levelUpMessage = '';

    // Get required services

    console.log(
      `Checking level up criteria for project ${project.id}, current level: ${currentLevel}`
    );

    switch (currentLevel) {
      case 1:
        // Check level 1 to 2 conditions (NFTs minted)
        const hasIdeaNFT = project.NFTs?.some((nft: any) => nft.type === 'idea');
        const hasVisionNFT = project.NFTs?.some((nft: any) => nft.type === 'vision');

        if (hasIdeaNFT && hasVisionNFT) {
          newLevel = 2;
          shouldLevelUp = true;
          levelUpMessage = `🎉 Congratulations! You've progressed to Level 2: Discord Setup.\n\nYour next steps are:\n- Create a Discord server for your community\n- Share the invite link with me\n- Add our verification bot\n- Grow to at least 4 members`;
        } else {
          console.log(
            `Project ${project.id} doesn't meet level 2 requirements: Idea NFT: ${hasIdeaNFT}, Vision NFT: ${hasVisionNFT}`
          );
        }
        break;

      case 2:
        // Check level 2 to 3 conditions (Discord created with members)
        const discordInfo = await DiscordService.getByProjectId(project.id);
        const founderUser = project.members?.find((m: any) => m.role === 'founder')?.bioUser;
        const discordConnected = founderUser?.discordId ? true : false;
        
        if (discordInfo && 
            discordInfo.verified && 
            discordInfo.memberCount && 
            discordInfo.memberCount >= 4 &&
            discordConnected) {
          // Check if we've recently sent this level-up notification
          if (wasLevelUpRecentlySent(project.id, 3)) {
            console.log(`Skipping duplicate level 3 notification for user ${project.id} (sent recently)`);
            return;
          }
          
          newLevel = 3;
          shouldLevelUp = true;
          levelUpMessage = `🎉 Congratulations! You've progressed to Level 3: Community Growth.\n\nYour next goals are:\n- Reach 5+ Discord members\n- Share 5+ scientific papers\n- Have 50+ messages in your Discord`;
        } else {
          const missingReqs = [];
          if (!discordInfo) missingReqs.push('Discord server not connected');
          else {
            if (!discordInfo.verified) missingReqs.push('Discord verification incomplete');
            if (discordInfo.memberCount && discordInfo.memberCount < 4)
              missingReqs.push(`Need more members (${discordInfo.memberCount}/4)`);
          }
          if (!discordConnected) missingReqs.push('Personal Discord account not connected');
          
          console.log(
            `Project ${project.id} doesn't meet level 3 requirements: ${missingReqs.join(', ')}`
          );
        }
        break;

      case 3:
        // Check level 3 to 4 conditions (Community growth metrics)
        const discordStats = await DiscordService.getByProjectId(project.id);
        if (
          discordStats &&
          discordStats.memberCount &&
          discordStats.memberCount >= 10 &&
          discordStats.papersShared >= 25 &&
          discordStats.messagesCount >= 100
        ) {
          // Check if we've recently sent this level-up notification
          if (wasLevelUpRecentlySent(project.id, 4)) {
            console.log(`Skipping duplicate level 4 notification for user ${project.id} (sent recently)`);
            return;
          }
          
          newLevel = 4;
          shouldLevelUp = true;
          levelUpMessage = `🎉 Congratulations! You've reached Level 4: Scientific Proof.\n\nYour next steps are:\n- Connect your Twitter account in settings [${config.app.url}/settings]\n- Create 3 introductory tweets about your BioDAO`;

          
          // Send sandbox email when reaching level 4
          try {
            // Send sandbox notification to Bio team members
            await sendSandboxEmail(project);
            console.log(`Sandbox email sent to Bio team for project ${project.id}`);
          } catch (emailError) {
            console.error(`Error sending sandbox email for project ${project.id}:`, emailError);
          }
        } else {
          const missingReqs = [];
          if (!discordStats) {
            missingReqs.push('Discord stats not available');
          } else {
            if (discordStats.memberCount && discordStats.memberCount < 10)
              missingReqs.push(`Need more members (${discordStats.memberCount}/10)`);
            if (discordStats.papersShared && discordStats.papersShared < 25)
              missingReqs.push(`Need more papers shared (${discordStats.papersShared}/25)`);
            if (discordStats.messagesCount && discordStats.messagesCount < 100)
              missingReqs.push(`Need more messages (${discordStats.messagesCount}/100)`);
          }
          console.log(
            `Project ${project.id} doesn't meet level 4 requirements: ${missingReqs.join(', ')}`
          );
        }
        break;

      case 4:
        // Check level 4 to 5 conditions (Twitter connected with intro tweets)
        const twitterData = await prisma.twitter.findUnique({
          where: { projectId: project.id }
        });
        
        if (twitterData && twitterData.connected && twitterData.introTweetsCount >= 3) {
          // Check if we've recently sent this level-up notification
          if (wasLevelUpRecentlySent(project.id, 5)) {
            console.log(`Skipping duplicate level 5 notification for user ${project.id} (sent recently)`);
            return;
          }
          
          newLevel = 5;
          shouldLevelUp = true;
          levelUpMessage = `🎉 Congratulations! You've reached Level 5: Social Presence.\n\nYour next goal is to grow your scientific community:\n- Recruit and verify at least 10 scientists or patients to your community \n- Host a Twitter Space`;
        } else {
          const missingReqs = [];
          if (!twitterData) missingReqs.push('Twitter account not connected');
          else {
            if (!twitterData.connected) missingReqs.push('Twitter connection not verified');
            if (twitterData.introTweetsCount < 3) 
              missingReqs.push(`Need more intro tweets (${twitterData.introTweetsCount}/3)`);
          }
          console.log(
            `Project ${project.id} doesn't meet level 5 requirements: ${missingReqs.join(', ')}`
          );
        }
        break;
        
      case 5:
        // Check level 5 to 6 conditions (Verified scientists count AND Twitter Space)
        const hasTwitterSpace = (project.Twitter as any)?.twitterSpaceUrl ? true : false;
        
        if ((project as any).verifiedScientistCount >= 10 && hasTwitterSpace) {
          // Check if we've recently sent this level-up notification
          if (wasLevelUpRecentlySent(project.id, 6)) {
            console.log(`Skipping duplicate level 6 notification for user ${project.id} (sent recently)`);
            return;
          }
          
          newLevel = 6;
          shouldLevelUp = true;
          levelUpMessage = `🎉 Congratulations! You've reached Level 6: Scientific Community.\n\nYou've successfully built a community with 10+ verified scientists or patients and hosted a Twitter Space to engage your audience. This is a significant achievement for your BioDAO! \n\nThe Bio team will reach out to discuss next steps for your scientific community.`;
        } else {
          const missingReqs = [];
          if ((project as any).verifiedScientistCount < 10) {
            missingReqs.push(`verified scientists (${(project as any).verifiedScientistCount}/10)`);
          }
          if (!hasTwitterSpace) {
            missingReqs.push('Twitter Space not hosted');
          }
          console.log(
            `Project ${project.id} doesn't meet level 6 requirements: ${missingReqs.join(', ')}`
          );
        }
        break;

      case 6:
        // Check level 6 to 7 conditions (blogpost and Twitter thread)
        const hasBlogpost = (project.Twitter as any)?.blogpostUrl ? true : false;
        const hasTwitterThread = (project.Twitter as any)?.twitterThreadUrl ? true : false;
        
        if (hasBlogpost && hasTwitterThread) {
          // Check if we've recently sent this level-up notification
          if (wasLevelUpRecentlySent(project.id, 7)) {
            console.log(`Skipping duplicate level 7 notification for user ${project.id} (sent recently)`);
            return;
          }
          
          newLevel = 7;
          shouldLevelUp = true;
          levelUpMessage = `🎉 **Congratulations on reaching Level 7!**\n\nYou've successfully articulated your BioDAO's vision through both a comprehensive blogpost and an engaging Twitter thread. This is the final level of the BioDAO onboarding process.\n\nYour BioDAO is now fully established! You have completed all onboarding requirements. The Bio team will be in touch regarding next steps and opportunities within the ecosystem.`;
        } else {
          const missingReqs = [];
          if (!hasBlogpost) missingReqs.push('visionary blogpost');
          if (!hasTwitterThread) missingReqs.push('Twitter thread');
          
          console.log(
            `Project ${project.id} doesn't meet level 7 requirements: Missing ${missingReqs.join(', ')}`
          );
        }
        break;
        
      case 7:
        // Level 7 is the final level - no further level-up needed
        console.log(`Project ${project.id} is at the final level (Level 7)`);
        break;

      default:
        console.log(`Project ${project.id} is already at max level (${currentLevel})`);
        break;
    }

    // Perform the level up if conditions are met
    if (shouldLevelUp && newLevel > currentLevel) {
      // Update the level in the database
      const updatedProject = await prisma.project.update({
        where: { id: project.id },
        data: { level: newLevel },
      });

      console.log(`Project ${project.id} leveled up from ${currentLevel} to ${newLevel}`);

      // Send level up notification to the client
      ws.send(
        JSON.stringify({
          type: 'level_up',
          previousLevel: currentLevel,
          newLevel: newLevel,
        })
      );

      // Get or create chat session for this user
      const sessionId = await ChatSessionService.getOrCreateForUser(project.id);
      
      // Save the level up message to chat history
      await ChatMessageService.saveMessage(sessionId, levelUpMessage, true, 'LEVEL_UP', true);

      // Also send a message about leveling up
      ws.send(
        JSON.stringify({
          type: 'message',
          content: levelUpMessage,
          isFromAgent: true,
          action: 'level_up',
        })
      );

      // Record that we've sent this level-up notification
      recordLevelUpSent(project.id, newLevel);

      // Send level up email notification
      try {
        if (project.members && project.members.length > 0 && project.members[0].bioUser.email) {
          await sendLevelUpEmail(project.members[0].bioUser.email, newLevel);
          console.log(`Level up email sent to ${project.members[0].bioUser.email} for level ${newLevel}`);
        }
      } catch (emailError) {
        console.error(`Error sending level up email for project ${project.id}:`, emailError);
      }
    }
  } catch (error) {
    console.error('Error checking and performing level up:', error);
    throw error;
  }
}

/**
 * Send a level up congratulatory email to the user
 */
async function sendLevelUpEmail(userEmail: string, level: number) {
  const EmailService = require('../services/email.service');
  try {
    await EmailService.sendLevelUpEmail(userEmail, level);
    console.log(`Level up email sent to ${userEmail} for level ${level}`);
  } catch (error) {
    console.error(`Error sending level up email to ${userEmail}:`, error);
    throw error;
  }
}

/**
 * Send level-up emails to all project members
 * @param project Project with members data
 * @param level The new level reached
 */
async function sendLevelUpEmailsToAllMembers(project: any, level: number): Promise<void> {
  if (!project.members || project.members.length === 0) {
    console.warn(`No members found for project ${project.id}, skipping level-up emails`);
    return;
  }

  const emailPromises: Promise<void>[] = [];
  
  for (const member of project.members) {
    if (member.bioUser && member.bioUser.email) {
      const emailPromise = sendLevelUpEmail(member.bioUser.email, level)
        .catch(error => {
          console.error(`Error sending level up email to ${member.bioUser.email} for project ${project.id}:`, error);
          // Don't throw here, we want to continue sending to other members
        });
      emailPromises.push(emailPromise);
    } else {
      console.warn(`Member ${member.id} in project ${project.id} has no email address, skipping level-up email`);
    }
  }

  try {
    await Promise.all(emailPromises);
    console.log(`Level-up emails sent to ${emailPromises.length} members for project ${project.id} reaching level ${level}`);
  } catch (error) {
    console.error(`Error sending some level-up emails for project ${project.id}:`, error);
  }
}

/**
 * Send a notification email to the Bio team when a user reaches the sandbox level
 */
async function sendSandboxEmail(project: any) {
  const EmailService = require('../services/email.service');
  try {
    // Get the sandbox notification emails from environment variable
    const sandboxEmails = process.env.SANDBOX_NOTIFICATION_EMAIL;
    
    if (!sandboxEmails) {
      console.warn('SANDBOX_NOTIFICATION_EMAIL environment variable not set, skipping sandbox email');
      return;
    }
    
    // Parse the comma-separated email list
    const emailList = sandboxEmails.split(',').map(email => email.trim()).filter(email => email);
    
    if (emailList.length === 0) {
      console.warn('No valid emails found in SANDBOX_NOTIFICATION_EMAIL environment variable');
      return;
    }
    
    // Send sandbox notification to each team member
    for (const email of emailList) {
      try {
        await EmailService.sendSandboxEmail(project, email);
        console.log(`Sandbox email sent to ${email} for project ${project.id}`);
      } catch (emailError) {
        console.error(`Error sending sandbox email to ${email} for project ${project.id}:`, emailError);
      }
    }
    
    console.log(`Sandbox email notifications sent to ${emailList.length} team members for project ${project.id}`);
  } catch (error) {
    console.error(`Error sending sandbox email for project ${project.id}:`, error);
    throw error;
  }
}

/**
 * Handle notification when a bot is successfully installed to a Discord server
 * @param ws WebSocket connection
 * @param userId User ID
 * @param serverDetails Discord server details
 */
async function handleBotInstalled(
  ws: WebSocket,
  userId: string,
  serverDetails: {
    guildId: string;
    guildName?: string;
    memberCount: number;
  }
): Promise<void> {
  try {
    // Get or create chat session first to avoid reference before declaration
    const sessionId = await getOrCreateChatSession(userId);

    // Get the project to check for level-up conditions
    const project = await prisma.project.findUnique({
      where: { id: userId },
      include: {
        Discord: true,
        NFTs: true,
        members: { include: { bioUser: true } }, // Added this line
      },
    });

    // Get the Discord record
    const discordRecord = await prisma.discord.findFirst({
      where: {
        serverId: serverDetails.guildId,
        projectId: userId,
      },
    });

    if (!discordRecord) {
      console.error(
        `No Discord record found for server ID: ${serverDetails.guildId} and project ID: ${userId}`
      );
      return;
    }

    // Update the record to mark the bot as added
    await prisma.discord.update({
      where: { id: discordRecord.id },
      data: {
        botAdded: true,
        botAddedAt: new Date(),
        verified: true,
        memberCount: serverDetails.memberCount || discordRecord.memberCount,
      },
    });

    // Mark this server as having received a bot installation notification
    botInstallNotificationSent.set(serverDetails.guildId, true);

    // Create the bot added message
    const botAddedMessage = {
      content: `## Discord Bot Successfully Added! 🎉

**Great news!** The verification bot has been successfully added to your Discord server "${serverDetails.guildName || discordRecord.serverName || 'Your Discord Server'}".

### Current Stats:
- **Members:** ${serverDetails.memberCount} ${serverDetails.memberCount === 1 ? 'member' : 'members'}
- **Messages:** 0 (tracking starts now)
- **Papers shared:** 0 (tracking starts now)

### What This Means:
- ✅ Your Discord server is now **fully verified**
- ✅ Member counts and activity are being **automatically tracked**
- ✅ Scientific papers shared in the server will be **detected and counted**
- ✅ All metrics will update in **real-time** towards your level progression

${serverDetails.memberCount >= 4 ? '**Congratulations!** You have enough members to qualify for Level 3!' : `### Next Steps:\nYou need **${4 - serverDetails.memberCount} more ${4 - serverDetails.memberCount === 1 ? 'member' : 'members'}** to reach Level 3.\n\nKeep growing your community by inviting researchers and collaborators to join your server!`}`,
    };

    // Save the message to the chat history
    //await saveChatMessage(sessionId, botAddedMessage, true, 'BOT_ADDED', true);

    // Send the notification over WebSocket
    /*
    ws.send(
      JSON.stringify({
        type: 'message',
        content: botAddedMessage.content,
        action: 'BOT_ADDED',
      })
    );

    // Also send a dedicated message type for bot installation
    ws.send(
      JSON.stringify({
        type: 'discord_bot_installed',
        discord: {
          memberCount: serverDetails.memberCount,
          papersShared: 0,
          messagesCount: 0,
          qualityScore: 0,
          verified: true,
          serverName: serverDetails.guildName || discordRecord.serverName || 'Your Discord Server',
          botAdded: true,
        },
      })
    );
    */

    // After bot is installed, always check level-up conditions using the proper mechanism
    if (project) {
      // Pass in the updated Discord stats for immediate level-up check
      await checkLevelUpConditions(
        userId, 
        project.level, 
        {
          verified: true,
          memberCount: serverDetails.memberCount,
          papersShared: 0,
          messagesCount: 0,
          qualityScore: 0,
          botAdded: true
        },
        ws
      );
    }

    console.log(
      `Discord bot installation processed for user ${userId}, server: ${serverDetails.guildName || serverDetails.guildId}`
    );
    return;
  } catch (error) {
    console.error('Error handling bot installation:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to process Discord bot installation',
      })
    );
    return;
  }
}

/**
 * Handles when a Discord bot is added to a new server
 * This function is called from the Discord bot's guildCreate event
 *
 * @param guildId The ID of the guild that was created
 * @param guildName The name of the guild
 * @param memberCount The current member count of the guild
 */
async function handleGuildCreate(
  guildId: string,
  guildName: string,
  memberCount: number
): Promise<void> {
  console.log(`[WS Service - handleGuildCreate] Received event for guild: ${guildName} (${guildId}), members: ${memberCount}`);
  try {
    console.log(`[WS Service - handleGuildCreate] Searching for Discord record with serverId: ${guildId}`);
    // Find the Discord record internally
    const discordRecord = await prisma.discord.findFirst({
      where: { serverId: guildId },
    });

    if (discordRecord) {
      console.log(`[WS Service - handleGuildCreate] Found Discord record ID: ${discordRecord.id} for serverId: ${guildId}. ProjectId: ${discordRecord.projectId}`);
      console.log(`[WS Service - handleGuildCreate] Attempting to update record ID: ${discordRecord.id} with botAdded: true, verified: true, memberCount: ${memberCount}`);

      // Update the record directly
      await prisma.discord.update({
        where: { id: discordRecord.id },
        data: {
          botAdded: true,
          botAddedAt: new Date(),
          memberCount: memberCount,
          verified: true, // Mark as verified upon successful bot addition
        },
      });
      console.log(`[WS Service - handleGuildCreate] Successfully updated record ID: ${discordRecord.id}.`);

      // Get user and send notification
      console.log(`[WS Service - handleGuildCreate] Looking up project with ID: ${discordRecord.projectId}`);
      const project = await prisma.project.findUnique({
        where: { id: discordRecord.projectId },
        include: { Discord: true, NFTs: true, members: { include: { bioUser: true } } }, // Added members relation here
      });

      if (project) {
        console.log(`[WS Service - handleGuildCreate] Found project: ${project.id}`);
        const sessionId = await getOrCreateChatSession(project.id);
        // Find the user's WebSocket connection
        const ws = activeConnections[project.id];

        // Check if we've already sent a bot installation notification for this server
        const alreadySentNotification = botInstallNotificationSent.get(guildId) || false;

        // Only send the notification if we haven't already sent one
        if (!alreadySentNotification && ws && ws.readyState === WebSocket.OPEN) {
          console.log(`Sending bot installation notification to user ${project.id}`);

          const botAddedMessage = {
            content: `## Discord Bot Successfully Added! 🎉

**Great news!** The verification bot has been successfully added to your Discord server "${guildName || 'Your Discord Server'}".

### Current Stats:
- **Members:** ${memberCount} ${memberCount === 1 ? 'member' : 'members'}
- **Messages:** 0 (tracking starts now)
- **Papers shared:** 0 (tracking starts now)

### What This Means:
- ✅ Your Discord server is now **fully verified**
- ✅ Member counts and activity are being **automatically tracked**
- ✅ Scientific papers shared in the server will be **detected and counted**
- ✅ All metrics will update in **real-time** towards your level progression

${memberCount >= 4 ? '**Congratulations!** You have enough members to qualify for Level 3!' : `### Next Steps:\nYou need **${4 - memberCount} more ${4 - memberCount === 1 ? 'member' : 'members'}** to reach Level 3.\n\nKeep growing your community by inviting researchers and collaborators to join your server!`}`,
          };

          await saveChatMessage(sessionId, botAddedMessage, true, 'BOT_ADDED', true);

          // Send the text message
          ws.send(
            JSON.stringify({
              type: 'message',
              content: botAddedMessage.content,
              action: 'BOT_ADDED',
              isFromAgent: true,
            })
          );

          // Also send the dedicated message type for Discord stats update
          ws.send(
            JSON.stringify({
              type: 'discord_bot_installed',
              discord: {
                memberCount: memberCount,
                papersShared: discordRecord.papersShared || 0, // Use current values or 0
                messagesCount: discordRecord.messagesCount || 0,
                qualityScore: discordRecord.qualityScore || 0,
                verified: true,
                serverName: guildName || discordRecord.serverName || 'Your Discord Server',
                botAdded: true,
                serverId: guildId,
              },
            })
          );
          
          // Mark this server as having received a notification
          botInstallNotificationSent.set(guildId, true);
        }
        else if (alreadySentNotification) {
          console.log(`Skipping duplicate bot installation notification for server ${guildId}, already sent`);
        }
        
        // Even if we don't send a notification, still check for level up
        if (ws && ws.readyState === WebSocket.OPEN) {
          // Check for level up using the standardized mechanism
          await checkLevelUpConditions(
            project.id,
            project.level,
            {
              verified: true,
              memberCount: memberCount,
              papersShared: 0,
              messagesCount: 0,
              qualityScore: 0,
              botAdded: true,
              serverId: guildId
            },
            ws
          );
        } else {
          console.log(
            `User ${project.id} does not have an active WebSocket connection. Cannot check for level up.`
          );
        }
      }

      console.log(
        `[WS Service - handleGuildCreate] Successfully processed bot addition for Discord server ID: ${discordRecord.serverId}`
      );
    } else {
      console.warn(`[WS Service - handleGuildCreate] Could not find Discord record for server ID: ${guildId}. Was setup initiated via chat first?`);
    }
  } catch (error) {
    console.error(`[WS Service - handleGuildCreate] Failed to process guildCreate event for guildId ${guildId}:`, error);
  }
}

// Function to check and update user level based on Discord stats
async function checkAndUpdateUserLevel(project: any) {
  // Skip if user is already at the maximum level
  if (project.level >= 4) return;

  // Level 3 requires Discord creation and 4 members
  if (
    project.level === 2 &&
    project.Discord &&
    project.Discord.botAdded &&
    project.Discord.memberCount >= 4
  ) {
    const newLevel = 3;
    
    // Check if we've recently sent this level-up notification
    if (wasLevelUpRecentlySent(project.id, newLevel)) {
      console.log(`Skipping duplicate level ${newLevel} notification for user ${project.id} (sent recently)`);
      return;
    }
    
    await prisma.project.update({
      where: { id: project.id },
      data: { level: newLevel },
    });

    // Get session for chat history
    const sessionId = await getOrCreateChatSession(project.id);
    
    // Create a properly formatted level-up message that matches other level-up notifications
    const levelUpMessage = `## Level ${newLevel} Unlocked! 🎉

**Congratulations!** Your BioDAO community now has **${project.Discord.memberCount} members**, which means you've advanced to **Level ${newLevel}!**

To advance to Level 4, you'll need to:
1. **Grow your community to 10+ members**
2. **Share 25+ scientific papers** in your Discord
3. **Reach 100+ quality messages**

I'll help you track these metrics and provide strategies to achieve them.`;

    // Save level-up message to chat
    await saveChatMessage(sessionId, levelUpMessage, true, 'LEVEL_UP', true);

    // Send notification if user is connected
    const userConnection = activeConnections[project.id];
    if (userConnection) {
      // Send level-up message as a regular chat message
      userConnection.send(
        JSON.stringify({
          type: 'message',
          content: levelUpMessage,
          isFromAgent: true,
          action: 'LEVEL_UP',
        })
      );
      
      // Also send dedicated level-up event with proper metadata
      userConnection.send(
        JSON.stringify({
          type: 'level_up',
          previousLevel: 2,
          newLevel: newLevel,
          message: levelUpMessage,
          nextLevelRequirements: [
            'Grow your community to 10+ Discord members',
            'Share at least 25 scientific papers in your server',
            'Reach 100+ quality messages in your community',
          ],
        })
      );
    }

    // Record that we've sent this level-up notification
    recordLevelUpSent(project.id, newLevel);

    // Send level up email
    if (project.members && project.members.length > 0 && project.members[0].bioUser.email) {
      try {
        await sendLevelUpEmail(project.members[0].bioUser.email, newLevel);
      } catch (error) {
        console.error(`Error sending level up email to ${project.members[0].bioUser.email}:`, error);
      }
    }
  }

  // Level 4 requires 10 Discord members, 25 papers, 100 messages
  if (
    project.level === 3 &&
    project.Discord &&
    project.Discord.memberCount >= 10 &&
    project.Discord.papersShared >= 25 &&
    project.Discord.messagesCount >= 100
  ) {
    const newLevel = 4;
    
    // Check if we've recently sent this level-up notification
    if (wasLevelUpRecentlySent(project.id, newLevel)) {
      console.log(`Skipping duplicate level ${newLevel} notification for user ${project.id} (sent recently)`);
      return;
    }
    
    await prisma.project.update({
      where: { id: project.id },
      data: { level: newLevel },
    });

    // Get session for chat history
    const sessionId = await getOrCreateChatSession(project.id);
    
    // Create a properly formatted level-up message that matches other level-up notifications
    const levelUpMessage = `## Level ${newLevel} Unlocked! 🎉

**Congratulations!** Your BioDAO community has reached critical mass with:
- **${project.Discord.memberCount} members**
- **${project.Discord.papersShared} scientific papers shared**
- **${project.Discord.messagesCount} messages** in your server

You've advanced to **Level ${newLevel}** and now have access to the BioDAO sandbox!

The Bio team will contact you via email shortly to schedule a call to discuss your next steps.`;

    // Save level-up message to chat
    await saveChatMessage(sessionId, levelUpMessage, true, 'LEVEL_UP', true);

    // Send notification if user is connected
    const userConnection = activeConnections[project.id];
    if (userConnection) {
      // Send level-up message as a regular chat message
      userConnection.send(
        JSON.stringify({
          type: 'message',
          content: levelUpMessage,
          isFromAgent: true,
          action: 'LEVEL_UP',
        })
      );
      
      // Also send dedicated level-up event with proper metadata
      userConnection.send(
        JSON.stringify({
          type: 'level_up',
          previousLevel: 3,
          newLevel: newLevel,
          message: levelUpMessage,
          nextLevelRequirements: [
            'All requirements completed - congratulations!',
            'The Bio team will contact you to schedule a call',
            'You now have access to the full BioDAO sandbox',
          ],
        })
      );
    }

    // Record that we've sent this level-up notification
    recordLevelUpSent(project.id, newLevel);

    // Send email notifications
    if (project.members && project.members.length > 0 && project.members[0].bioUser.email) {
      try {
        // Send level up email
        await sendLevelUpEmail(project.members[0].bioUser.email, newLevel);
        
        // Send sandbox email for final level
        await sendSandboxEmail(project);
      } catch (error) {
        console.error(`Error sending emails to ${project.members[0].bioUser.email}:`, error);
      }
    } else if (process.env.SANDBOX_NOTIFICATION_EMAIL) {
      // If no user email but sandbox notifications are configured, still send sandbox email
      try {
        await sendSandboxEmail(project);
      } catch (error) {
        console.error(`Error sending sandbox email for project ${project.id}:`, error);
      }
    }
  }
}

// Utility: Normalize markdown for clean rendering (same as in AI)
function normalizeMarkdown(content: string): string {
  if (!content) return '';
  let normalized = content.trim();
  normalized = normalized.replace(/\n{3,}/g, '\n\n');
  normalized = normalized.replace(/(\d+\.)(?:\n\s*){2,}(?=\d+\.)/g, '$1\n');
  normalized = normalized.replace(/(\* .+)(\n)(?!\n|\* )/g, '$1\n');
  normalized = normalized.replace(/^(\s*\n)+|((\n\s*)+)$/g, '');
  return normalized;
}

// Function to check if a level-up notification was recently sent
function wasLevelUpRecentlySent(userId: string, newLevel: number): boolean {
  const now = Date.now();
  const userLevelUps = recentLevelUpsByUser.get(userId);
  
  if (!userLevelUps) return false;
  
  const lastSentTimestamp = userLevelUps.get(newLevel);
  if (!lastSentTimestamp) return false;
  
  // Consider a level-up notification as "recent" if it was sent in the last 60 seconds
  return (now - lastSentTimestamp) < 30000; // 60 seconds
}

// Function to record that a level-up notification was sent
function recordLevelUpSent(userId: string, newLevel: number): void {
  let userLevelUps = recentLevelUpsByUser.get(userId);
  
  if (!userLevelUps) {
    userLevelUps = new Map<number, number>();
    recentLevelUpsByUser.set(userId, userLevelUps);
  }
  
  userLevelUps.set(newLevel, Date.now());
}

/**
 * Handle Twitter account connection
 * @param ws WebSocket connection
 * @param userId User ID
 * @param twitterData Twitter account data
 */
async function handleTwitterConnect(
  ws: WebSocket,
  userId: string,
  twitterData: { twitterId: string; twitterUsername: string }
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    if (!twitterData.twitterId || !twitterData.twitterUsername) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Twitter ID and username are required',
        })
      );
      return;
    }

    // Connect Twitter account
    const updatedTwitterData = await TwitterService.connectAccount(
      userId,
      twitterData.twitterId,
      twitterData.twitterUsername
    );

    // Get or create chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Save chat message about Twitter connection
    await saveChatMessage(
      sessionId,
      `Twitter account @${twitterData.twitterUsername} connected successfully`,
      true,
      'twitter_connected',
      true
    );

    // Send success message
    ws.send(
      JSON.stringify({
        type: 'twitter_connected',
        twitter: updatedTwitterData,
        message: `Twitter account @${twitterData.twitterUsername} connected successfully`
      })
    );

    // Send as regular message
    ws.send(
      JSON.stringify({
        type: 'message',
        content: `## Twitter Account Connected! 🎉\n\nYour Twitter account **@${twitterData.twitterUsername}** has been connected successfully to your BioDAO.\n\n**Next Step:**\nPublish 3 introductory tweets about your DAO and its mission. These should focus on:\n1. Your core scientific mission\n2. The problems your community aims to solve\n3. Inviting other researchers to join\n\nInclude hashtags like #DeSci, #BioDAO, and your research field to increase visibility.`,
        isFromAgent: true,
        action: 'twitter_connected'
      })
    );

    // Check for level-up conditions
    const project = await ProjectService.getById(userId);
    if (project) {
      await checkAndPerformLevelUp(project, ws);
    }

  } catch (error) {
    console.error('Error connecting Twitter account:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to connect Twitter account',
      })
    );
  }
}

/**
 * Handle verification of Twitter introductory tweets
 * @param ws WebSocket connection
 * @param userId User ID
 */
async function handleVerifyTwitterTweets(
  ws: WebSocket,
  userId: string
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    // Verify tweets
    const tweetCount = await TwitterService.verifyIntroTweets(userId);
    
    // Get updated Twitter data
    const twitterData = await TwitterService.getByProjectId(userId);
    
    if (!twitterData) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Twitter account not connected',
        })
      );
      return;
    }

    // Get or create chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Save chat message about tweet verification
    let verificationMessage = '';
    
    if (tweetCount >= 3) {
      verificationMessage = `Verified ${tweetCount} introductory tweets about your DAO! You've completed this requirement for Level 5.`;
      
      await saveChatMessage(
        sessionId,
        verificationMessage,
        true,
        'twitter_tweets_verified',
        true
      );
    } else {
      verificationMessage = `Found ${tweetCount}/3 introductory tweets about your DAO. You need ${3 - tweetCount} more tweets to complete this requirement.`;
      
      await saveChatMessage(
        sessionId,
        verificationMessage,
        true,
        'twitter_tweets_partial',
        true
      );
    }

    // Send success message
    ws.send(
      JSON.stringify({
        type: 'twitter_tweets_verified',
        tweetCount,
        verified: tweetCount >= 3,
        twitter: twitterData,
        message: verificationMessage
      })
    );

    // Send as regular message
    ws.send(
      JSON.stringify({
        type: 'message',
        content: `## Twitter Verification Update\n\n${verificationMessage}`,
        isFromAgent: true,
        action: tweetCount >= 3 ? 'twitter_tweets_verified' : 'twitter_tweets_partial'
      })
    );

    // If tweets are verified, check for level-up conditions
    if (tweetCount >= 3) {
      const project = await ProjectService.getById(userId);
      if (project) {
        await checkAndPerformLevelUp(project, ws);
      }
    }

  } catch (error) {
    console.error('Error verifying Twitter tweets:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to verify Twitter tweets',
      })
    );
  }
}

/**
 * Handle submission of specific Twitter tweets for verification
 * @param ws WebSocket connection
 * @param userId User ID
 * @param tweetUrls Array of tweet URLs to verify
 */
async function handleSubmitTwitterTweets(
  ws: WebSocket,
  userId: string,
  tweetUrls: string[]
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    if (!tweetUrls || !Array.isArray(tweetUrls) || tweetUrls.length === 0) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Please provide at least one tweet URL',
        })
      );
      return;
    }

    // Verify the submitted tweets
    const result = await TwitterService.verifySubmittedTweets(userId, tweetUrls);
    
    if (!result.success) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: result.error || 'Failed to verify tweets',
        })
      );
      return;
    }

    // Get or create chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Prepare appropriate message based on verification result
    let verificationMessage = '';
    let messageAction = '';
    const totalVerified = result.tweetCount;
    const newlyVerified = result.verifiedInThisRequest;
    
    if (totalVerified >= 3) {
      // All requirements met
      verificationMessage = `Great! I've verified ${newlyVerified} new tweet${newlyVerified !== 1 ? 's' : ''} about your DAO. You now have ${totalVerified}/3 verified tweets. You've completed this requirement for Level 5! 🎉`;
      messageAction = 'twitter_tweets_verified';
    } else {
      // Still need more tweets
      verificationMessage = `I've verified ${newlyVerified} new tweet${newlyVerified !== 1 ? 's' : ''} about your DAO. You now have ${totalVerified}/3 verified tweets. You need ${3 - totalVerified} more to complete this requirement.`;
      messageAction = 'twitter_tweets_partial';
    }
    
    // Save chat message about tweet verification
    await saveChatMessage(
      sessionId,
      verificationMessage,
      true,
      messageAction,
      true
    );

    // Send success message
    ws.send(
      JSON.stringify({
        type: 'twitter_tweets_verified',
        tweetCount: totalVerified,
        newlyVerified: newlyVerified,
        verified: totalVerified >= 3,
        twitter: result.twitterData,
        message: verificationMessage
      })
    );

    // Send as regular message
    ws.send(
      JSON.stringify({
        type: 'message',
        content: `## Twitter Verification Update\n\n${verificationMessage}${newlyVerified === 0 ? '\n\nNote: The tweets you submitted either weren\'t from your connected Twitter account or didn\'t contain relevant keywords about your BioDAO.' : ''}`,
        isFromAgent: true,
        action: messageAction
      })
    );

    // If tweets are verified, check for level-up conditions
    if (totalVerified >= 3) {
      const project = await ProjectService.getById(userId);
      if (project) {
        await checkAndPerformLevelUp(project, ws);
      }
    }

  } catch (error) {
    console.error('Error verifying submitted tweets:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to verify submitted tweets',
      })
    );
  }
}

/**
 * Get the count of verified scientists for a project
 * @param ws WebSocket connection
 * @param userId User ID
 */
async function handleGetVerifiedScientists(
  ws: WebSocket,
  userId: string
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    // Get the project with verified scientist count
    const project = await prisma.project.findUnique({
      where: { id: userId }
    });

    if (!project) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Project not found',
        })
      );
      return;
    }

    const discord = await prisma.discord.findFirst({
      where: { projectId: userId }
    });

    // Using type assertion to ensure TypeScript recognizes the field
    const scientistCount = (project as any).verifiedScientistCount || 0;

    // Send the verified scientists count
    ws.send(
      JSON.stringify({
        type: 'verified_scientists',
        count: scientistCount,
        totalNeeded: 10, // Level 6 requirement
        progress: Math.min(100, Math.round((scientistCount / 10) * 100)),
        memberCount: discord?.memberCount || 0,
      })
    );
  } catch (error) {
    console.error('Error fetching verified scientists:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to fetch verified scientists count',
      })
    );
  }
}

/**
 * Handle verification of a Twitter Space URL
 * @param ws WebSocket connection
 * @param userId User ID
 * @param spaceUrl Twitter Space URL to verify
 */
async function handleVerifyTwitterSpace(
  ws: WebSocket,
  userId: string,
  spaceUrl: string
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    if (!spaceUrl) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Twitter Space URL is required',
        })
      );
      return;
    }

    // Validate URL format
    if (!spaceUrl.includes('twitter.com/i/spaces/') && !spaceUrl.includes('x.com/i/spaces/')) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Invalid Twitter Space URL format. It should contain "twitter.com/i/spaces/" or "x.com/i/spaces/"',
        })
      );
      return;
    }

    // Get the Twitter record for this user
    const twitterRecord = await prisma.twitter.findUnique({
      where: { projectId: userId }
    });

    if (!twitterRecord) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Twitter account not connected. Please connect your Twitter account first.',
        })
      );
      return;
    }

    // Update the Twitter record with space URL and date
    await prisma.twitter.update({
      where: { id: twitterRecord.id },
      data: {
        twitterSpaceUrl: spaceUrl,
        twitterSpaceDate: new Date(),
      } as any // Use type assertion for the custom fields
    });

    // Get or create chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Save verification message to chat
    const verificationMessage = `Twitter Space successfully verified! URL: ${spaceUrl}`;
    await saveChatMessage(sessionId, verificationMessage, true, 'TWITTER_SPACE_VERIFIED', true);

    // Get the updated Twitter record
    const updatedTwitterRecord = await prisma.twitter.findUnique({
      where: { projectId: userId }
    });

    // Send success response
    ws.send(
      JSON.stringify({
        type: 'twitter_space_verified',
        success: true,
        twitterSpace: {
          url: (updatedTwitterRecord as any).twitterSpaceUrl,
          date: (updatedTwitterRecord as any).twitterSpaceDate,
        },
        message: verificationMessage
      })
    );

    // Also send as a chat message
    ws.send(
      JSON.stringify({
        type: 'message',
        content: `## Twitter Space Verified! 🎉\n\nYour Twitter Space has been successfully verified. This completes one of your Level 6 requirements.\n\nTwitter Space URL: ${spaceUrl}\nVerified on: ${new Date().toLocaleDateString()}`,
        isFromAgent: true,
        action: 'TWITTER_SPACE_VERIFIED'
      })
    );

    // Check if user now meets all Level 6 requirements and should level up
    const project = await ProjectService.getById(userId);
    if (project) {
      await checkAndPerformLevelUp(project, ws);
    }

  } catch (error) {
    console.error('Error verifying Twitter Space:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to verify Twitter Space',
      })
    );
  }
}

/**
 * Get the status of Twitter Space verification for a project
 * @param ws WebSocket connection
 * @param userId User ID
 */
async function handleGetTwitterSpaceStatus(
  ws: WebSocket,
  userId: string
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    // Get the Twitter record for this user
    const twitterRecord = await prisma.twitter.findUnique({
      where: { projectId: userId }
    });

    // Check if Twitter Space has been verified
    const isVerified = (twitterRecord as any)?.twitterSpaceUrl ? true : false;

    // Send the status
    ws.send(
      JSON.stringify({
        type: 'twitter_space_status',
        verified: isVerified,
        twitterSpace: isVerified ? {
          url: (twitterRecord as any)?.twitterSpaceUrl,
          date: (twitterRecord as any)?.twitterSpaceDate,
        } : null
      })
    );
  } catch (error) {
    console.error('Error getting Twitter Space status:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to get Twitter Space status',
      })
    );
  }
}

/**
 * Handle verification of a blogpost URL
 * @param ws WebSocket connection
 * @param userId User ID
 * @param blogpostUrl Blogpost URL to verify
 */
async function handleVerifyBlogpost(
  ws: WebSocket,
  userId: string,
  blogpostUrl: string
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    if (!blogpostUrl) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Blogpost URL is required',
        })
      );
      return;
    }

    // Validate URL format
    try {
      new URL(blogpostUrl);
    } catch (e) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Invalid URL format',
        })
      );
      return;
    }

    // Get the Twitter record for this user
    const twitterRecord = await prisma.twitter.findUnique({
      where: { projectId: userId }
    });

    if (!twitterRecord) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Twitter account not connected. Please connect your Twitter account first.',
        })
      );
      return;
    }

    // Update the Twitter record with blogpost URL
    await prisma.twitter.update({
      where: { id: twitterRecord.id },
      data: {
        blogpostUrl: blogpostUrl,
        blogpostDate: new Date(),
      } as any
    });

    // Get or create chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Save verification message to chat
    const verificationMessage = `Visionary blogpost successfully verified! URL: ${blogpostUrl}`;
    await saveChatMessage(sessionId, verificationMessage, true, 'BLOGPOST_VERIFIED', true);

    // Send success response
    ws.send(
      JSON.stringify({
        type: 'blogpost_verified',
        success: true,
        blogpost: {
          url: blogpostUrl,
          date: new Date(),
        },
        message: verificationMessage
      })
    );

    // Also send as a chat message
    ws.send(
      JSON.stringify({
        type: 'message',
        content: `## Visionary Blogpost Verified! 🎉\n\nYour blogpost about your BioDAO's future vision has been successfully verified. This completes one of your Level 7 requirements.\n\nBlogpost URL: ${blogpostUrl}\nVerified on: ${new Date().toLocaleDateString()}\n\nNext, you'll need to share this as a Twitter thread to complete Level 7. Would you like guidance on creating an effective thread based on your blogpost?`,
        isFromAgent: true,
        action: 'BLOGPOST_VERIFIED'
      })
    );

    // Check if user now meets all Level 7 requirements and should level up
    const project = await ProjectService.getById(userId);
    if (project) {
      await checkAndPerformLevelUp(project, ws);
    }

  } catch (error) {
    console.error('Error verifying blogpost:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to verify blogpost',
      })
    );
  }
}

/**
 * Handle verification of a Twitter thread URL
 * @param ws WebSocket connection
 * @param userId User ID
 * @param threadUrl Twitter thread URL to verify
 */
async function handleVerifyTwitterThread(
  ws: WebSocket,
  userId: string,
  threadUrl: string
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    if (!threadUrl) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Twitter thread URL is required',
        })
      );
      return;
    }

    // Validate URL format (either twitter.com or x.com)
    if (!threadUrl.includes('twitter.com/') && !threadUrl.includes('x.com/')) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Invalid Twitter URL format. It should contain "twitter.com/" or "x.com/"',
        })
      );
      return;
    }

    // Get the Twitter record for this user
    const twitterRecord = await prisma.twitter.findUnique({
      where: { projectId: userId }
    });

    if (!twitterRecord) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Twitter account not connected. Please connect your Twitter account first.',
        })
      );
      return;
    }

    // Check if blogpost was verified first
    if (!(twitterRecord as any).blogpostUrl) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Please verify your visionary blogpost first before verifying your Twitter thread.',
        })
      );
      return;
    }

    // Update the Twitter record with thread URL
    await prisma.twitter.update({
      where: { id: twitterRecord.id },
      data: {
        twitterThreadUrl: threadUrl,
        twitterThreadDate: new Date(),
      } as any
    });

    // Get or create chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Save verification message to chat
    const verificationMessage = `Twitter thread successfully verified! URL: ${threadUrl}`;
    await saveChatMessage(sessionId, verificationMessage, true, 'TWITTER_THREAD_VERIFIED', true);

    // Send success response
    ws.send(
      JSON.stringify({
        type: 'twitter_thread_verified',
        success: true,
        twitterThread: {
          url: threadUrl,
          date: new Date(),
        },
        message: verificationMessage
      })
    );

    // Also send as a chat message
    ws.send(
      JSON.stringify({
        type: 'message',
        content: `## Twitter Thread Verified! 🎉\n\nYour Twitter thread sharing your BioDAO's vision has been successfully verified. Congratulations! You have now completed all Level 7 requirements.\n\nTwitter Thread URL: ${threadUrl}\nVerified on: ${new Date().toLocaleDateString()}`,
        isFromAgent: true,
        action: 'TWITTER_THREAD_VERIFIED'
      })
    );

    // Check if user now meets all Level 7 requirements and should level up
    const project = await ProjectService.getById(userId);
    if (project) {
      await checkAndPerformLevelUp(project, ws);
    }

  } catch (error) {
    console.error('Error verifying Twitter thread:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to verify Twitter thread',
      })
    );
  }
}

/**
 * Get the status of blogpost and Twitter thread verification
 * @param ws WebSocket connection
 * @param userId User ID
 */
async function handleGetBlogpostStatus(
  ws: WebSocket,
  userId: string
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    // Get the Twitter record for this user
    const twitterRecord = await prisma.twitter.findUnique({
      where: { projectId: userId }
    });

    // Check verification status
    const blogpostVerified = (twitterRecord as any)?.blogpostUrl ? true : false;
    const threadVerified = (twitterRecord as any)?.twitterThreadUrl ? true : false;

    // Send the status
    ws.send(
      JSON.stringify({
        type: 'blogpost_status',
        blogpostVerified: blogpostVerified,
        threadVerified: threadVerified,
        blogpost: blogpostVerified ? {
          url: (twitterRecord as any)?.blogpostUrl,
          date: (twitterRecord as any)?.blogpostDate,
        } : null,
        twitterThread: threadVerified ? {
          url: (twitterRecord as any)?.twitterThreadUrl,
          date: (twitterRecord as any)?.twitterThreadDate,
        } : null
      })
    );
  } catch (error) {
    console.error('Error getting blogpost status:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to get blogpost status',
      })
    );
  }
}

/**
 * Handle verification of a Loom welcome video
 * @param ws WebSocket connection
 * @param userId User ID
 * @param loomVideoUrl Loom video URL to verify
 */
async function handleVerifyLoomVideo(
  ws: WebSocket,
  userId: string,
  loomVideoUrl: string
): Promise<void> {
  try {
    if (!userId) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'User ID is required',
        })
      );
      return;
    }

    if (!loomVideoUrl) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Loom video URL is required',
        })
      );
      return;
    }

    // Validate URL format - check if it's a Loom URL
    if (!loomVideoUrl.includes('loom.com/share/')) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Invalid Loom video URL format. It should contain "loom.com/share/"',
        })
      );
      return;
    }

    // Get the Twitter record for this user
    const twitterRecord = await prisma.twitter.findUnique({
      where: { projectId: userId }
    });

    if (!twitterRecord) {
      ws.send(
        JSON.stringify({
          type: 'error',
          message: 'Twitter account not connected. Please connect your Twitter account first.',
        })
      );
      return;
    }

    // Update the Twitter record with Loom video URL and date
    await prisma.twitter.update({
      where: { id: twitterRecord.id },
      data: {
        loomVideoUrl: loomVideoUrl,
        loomVideoDate: new Date(),
      } as any
    });

    // Get or create chat session
    const sessionId = await getOrCreateChatSession(userId);

    // Save verification message to chat
    const verificationMessage = `Loom welcome video successfully verified! URL: ${loomVideoUrl}`;
    await saveChatMessage(sessionId, verificationMessage, true, 'LOOM_VIDEO_VERIFIED', true);

    // Get the updated Twitter record
    const updatedTwitterRecord = await prisma.twitter.findUnique({
      where: { projectId: userId }
    });

    // Send success response
    ws.send(
      JSON.stringify({
        type: 'loom_video_verified',
        success: true,
        loomVideo: {
          url: updatedTwitterRecord?.loomVideoUrl,
          date: updatedTwitterRecord?.loomVideoDate,
        },
        message: verificationMessage
      })
    );

    // Also send as a chat message
    ws.send(
      JSON.stringify({
        type: 'message',
        content: `## Welcome Loom Video Verified! 🎉\n\nYour welcome video has been successfully verified. This completes another one of your Level 7 requirements.\n\nLoom Video URL: ${loomVideoUrl}\nVerified on: ${new Date().toLocaleDateString()}`,
        isFromAgent: true,
        action: 'LOOM_VIDEO_VERIFIED'
      })
    );

    // Check if user now meets all Level 7 requirements and should level up
    const project = await ProjectService.getById(userId);
    if (project) {
      await checkAndPerformLevelUp(project, ws);
    }

  } catch (error) {
    console.error('Error verifying Loom video:', error);
    ws.send(
      JSON.stringify({
        type: 'error',
        message: 'Failed to verify Loom video',
      })
    );
  }
}

/**
 * Extract blogpost URLs from a message (general URLs that aren't Twitter/X)
 * @param message User message
 * @returns Array of blogpost URLs found in the message
 */
function extractBlogpostUrls(message: string): string[] {
  const blogpostUrls: string[] = [];
  
  // Regular expression to match general URLs
  const urlRegex = /https?:\/\/[^\s]+/g;
  
  // Find all matches
  const matches = message.match(urlRegex);
  
  if (matches) {
    // Filter out Twitter/X URLs and duplicates
    const nonTwitterUrls = matches.filter(url => 
      !url.includes('twitter.com') && 
      !url.includes('x.com') &&
      !url.includes('loom.com')
    );
    const uniqueUrls = [...new Set(nonTwitterUrls)];
    blogpostUrls.push(...uniqueUrls);
  }
  
  return blogpostUrls;
}

/**
 * Extract Twitter thread URLs from a message
 * @param message User message
 * @returns Array of Twitter thread URLs found in the message
 */
function extractTwitterThreadUrls(message: string): string[] {
  const twitterThreadUrls: string[] = [];
  
  // Regular expressions to match Twitter URLs (same as tweet URLs since threads are also tweets)
  const twitterRegex = /https?:\/\/(www\.)?(twitter|x)\.com\/[a-zA-Z0-9_]+\/status\/\d+/g;
  
  // Find all matches
  const matches = message.match(twitterRegex);
  
  if (matches) {
    // Filter out duplicates
    const uniqueUrls = [...new Set(matches)];
    twitterThreadUrls.push(...uniqueUrls);
  }
  
  return twitterThreadUrls;
}

/**
 * Extract Loom video URLs from a message
 * @param message User message
 * @returns Array of Loom video URLs found in the message
 */
function extractLoomUrls(message: string): string[] {
  const loomUrls: string[] = [];
  
  // Regular expression to match Loom URLs
  const loomRegex = /https?:\/\/(www\.)?loom\.com\/share\/[a-zA-Z0-9]+/g;
  
  // Find all matches
  const matches = message.match(loomRegex);
  
  if (matches) {
    // Filter out duplicates
    const uniqueUrls = [...new Set(matches)];
    loomUrls.push(...uniqueUrls);
  }
  
  return loomUrls;
}

export {
  initWebSocketServer,
  activeConnections,
  handleBotInstalled,
  handleGuildCreate,
  checkAndPerformLevelUp,
  checkAndUpdateUserLevel,
  handleVerifyTwitterSpace,
  handleGetTwitterSpaceStatus,
};