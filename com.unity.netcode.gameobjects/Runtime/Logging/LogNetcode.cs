using System;
using Keepsake.Logging;
using Microsoft.Extensions.Logging;
using UnityEngine;
using ZLogger;

namespace Unity.Netcode
{
    /**
     * Creating your own logging category, for example "NPC":
     * 1. Copy the code of this class and put it in a new file named LogNPC.cs.
     * 2. Change the class name to LogNPC and update the type arguments in the ILogger Logger field to match your class name. The name of the class (LogNPC) is the name of the category.
     * 3. Open "Console Pro" preferences and add a Custom Filter for your new category, look at the existing Custom Filters to get an idea.
     *
     * This is quite manual right now, maybe we can make it nicer in the future. :-)
     */

    public class LogNetcode : ILogCategory
    {
        private static ILogger<LogNetcode> s_Logger = LogManager.GetLogger<LogNetcode>();

        private static void Initialize()
        {
            s_Logger = LogManager.GetLogger<LogNetcode>();
        }


        // **********************************************************************
        // ******  You shouldn't have to change anything below this point  ******
        // **********************************************************************

        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.SubsystemRegistration)]
        private static void InitializeStatic()
        {
            LogManager.EnsureInitialized();
            Initialize();

            LogManager.m_ReinitializeLoggers -= Initialize;
            LogManager.m_ReinitializeLoggers += Initialize;
        }

        public static void Trace(string message)
        {
            s_Logger.ZLogTrace(message);
        }

        public static void Debug(string message)
        {
            s_Logger.ZLogDebug(message);
        }

        public static void Info(string message)
        {
            s_Logger.ZLogInformation(message);
        }

        public static void Warning(string message)
        {
            s_Logger.ZLogWarning(message);
        }

        public static void Error(string message)
        {
            s_Logger.ZLogError(message);
        }

        public static void Critical(string message)
        {
            s_Logger.ZLogCritical(message);
        }

        public static void Error(Exception exception, string message = null)
        {
            s_Logger.ZLogError(exception, message);
        }

        public static void Critical(Exception exception, string message = null)
        {
            s_Logger.ZLogCritical(exception, message);
        }
    }
}
