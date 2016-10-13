/*
 * Copyright 2016 Derek Weber
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.org.dcw.twitter.ingest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import twitter4j.RateLimitStatus;
import twitter4j.RateLimitStatusEvent;
import twitter4j.RateLimitStatusListener;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Application that collects the profiles of Twitter users.
 * <p>
 *
 * @see <a href=
 *      "https://github.com/yusuke/twitter4j/blob/master/twitter4j-examples/src/main/java/twitter4j/examples/json/SaveRawJSON.java">SaveRawJSON.java</a>
 * @see <a href=
 *      "https://dev.twitter.com/rest/reference/get/users/lookup">Twitter's
 *      <code>GET users/lookup</code> endpoint</a>
 */
@SuppressWarnings("static-access")
public final class TwitterProfilesRetrieverApp {
    private static Logger LOG = LoggerFactory.getLogger(TwitterProfilesRetrieverApp.class);
    private static final int FETCH_BATCH_SIZE = 100;
    private static final String FILE_SEPARATOR = System.getProperty("file.separator", "/");
    private static final Options OPTIONS = new Options();
    static {
        OPTIONS.addOption("i", "ids-file", true, "File of Twitter screen names or IDs (longs)");
        OPTIONS.addOption(longOpt("identifiers", "Inline, comma-delimited listing of Twitter screen names or IDs (longs) to look up (alternative to --screen-names-file)").hasArg().create());
        OPTIONS.addOption(longOpt("use-ids", "Expect Twitter IDs (longs) as input rather than screen names (default: false)").create());
        OPTIONS.addOption("o", "output-directory", true, "Directory to which to write profiles (default: ./profiles)");
        OPTIONS.addOption("c", "credentials", true, "File of Twitter credentials (default: ./twitter.properties)");
        OPTIONS.addOption("p", "include-protected", false, "Include protected accounts in ID listing (default: false)");
        OPTIONS.addOption("d", "debug", false, "Turn on debugging information (default: false)");
        OPTIONS.addOption("d", "debug", false, "Turn on debugging information (default: false)");
        OPTIONS.addOption("?", "help", false, "Ask for help with using this tool.");
    }


    private static OptionBuilder longOpt(String name, String description) {
        return OptionBuilder.withLongOpt(name).withDescription(description);
    }


    /**
     * Prints how the app ought to be used and causes the VM to exit.
     */
    private static void printUsageAndExit() {
        new HelpFormatter().printHelp("TwitterProfilesRetrieverApp", OPTIONS);
        System.exit(0);
    }


    public static void main(String[] args) throws IOException {
        final CommandLineParser parser = new BasicParser();
        String identifiers = null;
        String outputDir = "./output";
        String credentialsFile = "./twitter.properties";
        boolean debug = false;
        boolean includeProtected = false;
        boolean useIDs = false;
        try {
            final CommandLine cmd = parser.parse(OPTIONS, args);
            if (cmd.hasOption('i')) identifiers = cmd.getOptionValue('i');
            if (cmd.hasOption("identifiers")) identifiers = cmd.getOptionValue("identifiers");
            if (cmd.hasOption('o')) outputDir = cmd.getOptionValue('o');
            if (cmd.hasOption('c')) credentialsFile = cmd.getOptionValue('c');
            if (cmd.hasOption('d')) debug = true;
            if (cmd.hasOption("use-ids")) useIDs = true;
            if (cmd.hasOption('p')) includeProtected = true;
            if (cmd.hasOption('h')) printUsageAndExit();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        // check config
        if (identifiers == null) {
            printUsageAndExit();
        }

        new TwitterProfilesRetrieverApp().run(
            useIDs, identifiers, outputDir, credentialsFile, includeProtected, debug
        );
    }


    /**
     * Fetch the profiles specified by the given {@code givenScreenNames}
     * and write the profiles to the given {@code outputDir}. If any IDs cannot
     * be collected, they are written to "profiles-not-collected.txt". A mapping
     * of all IDs to screen names collected is written to "ids.txt".
     *
     * @param useIDs The identifiers will be Twitter IDs (longs) rather than String
     *         screen names.
     * @param givenIdentifiers Identifiers as a comma-delimited list OR as a path
     *         to file with identifiers, one per line.
     * @param outputDir Path to directory into which to write fetched profiles.
     * @param credentialsFile Path to properties file with Twitter credentials.
     * @param includeProtected When writing out the IDs and screen names of
     *         collected profiles, include the protected accounts.
     * @param debug Whether to increase debug logging.
     */
    public void run(
        final boolean useIDs,
        final String givenIdentifiers,
        final String outputDir,
        final String credentialsFile,
        final boolean includeProtected,
        final boolean debug
    ) throws IOException {

        LOG.info("Collecting profiles");
        LOG.info("  Twitter identifiers: {}", givenIdentifiers);
        LOG.info("  Identifiers, not screen names: {}", useIDs);
        LOG.info("  output directory: {}", outputDir);

        final List<String> identifiers = Lists.newArrayList();
        if (givenIdentifiers.contains(",")) {
            for (String id : givenIdentifiers.split(",")) {
                identifiers.add(id);
            }
        } else {
            identifiers.addAll(this.loadIdentifiersAsStrings(givenIdentifiers));
        }

        LOG.info("Read {} identifiers (screen names: {})", identifiers.size(), ! useIDs);

        if (!Files.exists(Paths.get(outputDir))) {
            LOG.info("Creating output directory {}", outputDir);
            Paths.get(outputDir).toFile().mkdirs();
        }

        final Configuration config =
            TwitterProfilesRetrieverApp.buildTwitterConfiguration(credentialsFile, debug);
        final Twitter twitter = new TwitterFactory(config).getInstance();
        twitter.addRateLimitStatusListener(this.rateLimitStatusListener);
        final ObjectMapper json = new ObjectMapper();

        final List<String> notCollected = new ArrayList<String>(identifiers);
        final Map<String, String> idScreenNameMap = Maps.newTreeMap();
        for (final List<String> batch : Lists.partition(identifiers, FETCH_BATCH_SIZE)) {

            try {
                LOG.info("Looking up {} users' profiles", batch.size());

                // ask Twitter for profiles
                ResponseList<User> profiles;
                if (useIDs) {
                    final long[] idArray = new long[batch.size()];
                    for (int i = 0; i < batch.size(); i++) {
                        idArray[i] = Long.parseLong(batch.get(i));
                    }
                    profiles = twitter.lookupUsers(idArray);
                } else {
                    final String[] allocation = new String[batch.size()];
                    profiles = twitter.lookupUsers(batch.toArray(allocation));
                }

                // extract the raw JSON
                final String rawJsonProfiles = TwitterObjectFactory.getRawJSON(profiles);

                // traverse the raw JSON (rather than the Twitter4j structures)
                final JsonNode profilesJsonNode = json.readTree(rawJsonProfiles);
                profilesJsonNode.forEach (profileNode ->  {

                    // extract vars for logging and filename creation
                    final String profileId = profileNode.get("id_str").asText();
                    final String screenName = profileNode.get("screen_name").asText();

                    if (useIDs) {
                        notCollected.remove(profileId);
                    } else {
                        notCollected.remove(screenName);
                    }

                    if (includeProtected || ! profileNode.get("protected").asBoolean()) {
                        idScreenNameMap.put(profileId, screenName);
                    }
                    if (profileNode.get("protected").asBoolean()) {
                        LOG.info("Profile @{} (#{}) is protected.", screenName, profileId);
                    }

                    final String fileName = outputDir + FILE_SEPARATOR + profileId + "-profile.json";

                    LOG.info("Profile @{} (#{}) -> {}", screenName, profileId, fileName);
                    try {
                        saveText(profileNode.toString(), fileName);
                    } catch (IOException e) {
                        LOG.warn("Failed to write to {}", fileName, e);
                    }
                });

            } catch (TwitterException e) {
                LOG.warn("Failed to communicate with Twitter", e);
            }
        }
        notCollected.stream().forEach(sn -> LOG.info("Did not collect profile for ID {}", sn));

        if (! notCollected.isEmpty()) {
            final String notCollectedReport = Joiner.on('\n').join(notCollected);// notCollected.stream().collect(Collectors.joining("\n"));
            saveText(notCollectedReport, outputDir + FILE_SEPARATOR + "profiles-not-collected.txt");
        }

        if (! idScreenNameMap.isEmpty()) {
            final String idCSV = idScreenNameMap.entrySet().stream()
                .map(e -> String.format("%s # @%s\n", e.getKey(), e.getValue()))
                .collect(Collectors.joining());
            saveText(idCSV, outputDir + FILE_SEPARATOR + "ids.txt");
        }
    }


    /**
     * Writes the given {@code text} to the specified file with UTF-8 encoding.
     *
     * @param text the text to persist
     * @param fileName the file (including path) to which to write the text
     * @throws IOException if there's a problem writing to the specified file
     */
    private static void saveText(final String text, final String fileName) throws IOException {
        try (final FileOutputStream fos = new FileOutputStream(fileName);
             final OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
             final BufferedWriter bw = new BufferedWriter(osw)) {
            bw.write(text);
            bw.flush();
        }
    }


    /**
     * Reads screen names from each line of the given file, ignoring lines starting
     * with # (comments) as well as trailing comments starting with # and eliminates duplicates.
     *
     * @param screenNamesFile File containing Twitter screen names, one per (non-comment) line.
     * @return A set of unique screen names.
     * @throws IOException If an error occurs reading the file.
     */
    private List<String> loadIdentifiersAsStrings(final String screenNamesFile) throws IOException {
        return Files.readAllLines(Paths.get(screenNamesFile)).stream()
            .map(l -> l.split("#")[0].trim())
            .filter(l -> l.length() > 0 && ! l.startsWith("#"))
            .map(sn -> (sn.startsWith("@") ? sn.substring(1): sn))
            .distinct()
            .collect(Collectors.toList());
    }


    /**
     * Listener to pay attention to when the Twitter's rate limit is being approached or breached.
     */
    final RateLimitStatusListener rateLimitStatusListener = new RateLimitStatusListener() {
        @Override
        public void onRateLimitStatus(final RateLimitStatusEvent event) {
            TwitterProfilesRetrieverApp.this.pauseIfNecessary(event.getRateLimitStatus());
        }

        @Override
        public void onRateLimitReached(final RateLimitStatusEvent event) {
            TwitterProfilesRetrieverApp.this.pauseIfNecessary(event.getRateLimitStatus());
        }
    };


    /**
     * If the provided {@link RateLimitStatus} indicates that we are about to break the rate
     * limit, in terms of number of calls or time window, then sleep for the rest of the period.
     *
     * @param status The current status of the our calls to Twitter
     */
    protected void pauseIfNecessary(final RateLimitStatus status) {
        if (status == null) {
            return;
        }

        final int secondsUntilReset = status.getSecondsUntilReset();
        final int callsRemaining = status.getRemaining();
        if (secondsUntilReset < 10 || callsRemaining < 10) {
            final int untilReset = status.getSecondsUntilReset() + 5;
            LOG.info("Rate limit reached. Waiting {} seconds starting at {}...", untilReset, new Date());
            try {
                Thread.sleep(untilReset * 1000);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while sleeping waiting for Twitter.", e);
                Thread.currentThread().interrupt(); // reset interrupt flag
            }
            LOG.info("Resuming...");
        }
    }


    /**
     * Builds the {@link Configuration} object with which to connect to Twitter,
     * including credentials and proxy information if it's specified.
     *
     * @param credentialsFile Property file of Twitter credentials.
     * @param debug Debug flag to pass to Twitter4j's config builder.
     * @return a Twitter4j {@link Configuration} object
     * @throws IOException if there's an error loading the application's
     *         {@link #credentialsFile}.
     */
    private static Configuration buildTwitterConfiguration
        (final String credentialsFile, final boolean debug) throws IOException {
        // TODO find a better name than credentials, given it might contain
        // proxy info
        final Properties credentials = loadCredentials(credentialsFile);

        final ConfigurationBuilder conf = new ConfigurationBuilder();
        conf.setJSONStoreEnabled(true).setDebugEnabled(debug)
            .setOAuthConsumerKey(credentials.getProperty("oauth.consumerKey"))
            .setOAuthConsumerSecret(credentials.getProperty("oauth.consumerSecret"))
            .setOAuthAccessToken(credentials.getProperty("oauth.accessToken"))
            .setOAuthAccessTokenSecret(credentials.getProperty("oauth.accessTokenSecret"));

        final Properties proxies = loadProxyProperties();
        if (proxies.containsKey("http.proxyHost")) {
            conf.setHttpProxyHost(proxies.getProperty("http.proxyHost"))
                .setHttpProxyPort(Integer.parseInt(proxies.getProperty("http.proxyPort")))
                .setHttpProxyUser(proxies.getProperty("http.proxyUser"))
                .setHttpProxyPassword(proxies.getProperty("http.proxyPassword"));
        }

        return conf.build();
    }


    /**
     * Loads the given {@code credentialsFile} from disk.
     *
     * @param credentialsFile the properties file with the Twitter credentials
     * @return A {@link Properties} map with the contents of credentialsFile
     * @throws IOException if there's a problem reading the credentialsFile.
     */
    private static Properties loadCredentials(final String credentialsFile) throws IOException {
        final Properties properties = new Properties();
        properties.load(Files.newBufferedReader(Paths.get(credentialsFile)));
        return properties;
    }


    /**
     * Loads proxy properties from {@code ./proxy.properties} and, if a password
     * is not supplied, asks for it in the console.
     *
     * @return A Properties instance filled with proxy information.
     */
    private static Properties loadProxyProperties() {
        final Properties properties = new Properties();
        final String proxyFile = "./proxy.properties";
        if (new File(proxyFile).exists()) {
            boolean success = true;
            try (Reader fileReader = Files.newBufferedReader(Paths.get(proxyFile))) {
                properties.load(fileReader);
            } catch (final IOException e) {
                System.err.printf("Attempted and failed to load %s: %s\n", proxyFile, e.getMessage());
                success = false;
            }
            if (success && !properties.containsKey("http.proxyPassword")) {
                final String message = "Please type in your proxy password: ";
                final char[] password = System.console().readPassword(message);
                properties.setProperty("http.proxyPassword", new String(password));
                properties.setProperty("https.proxyPassword", new String(password));
            }
            properties.forEach((k, v) -> System.setProperty(k.toString(), v.toString()));
        }
        return properties;
    }
}
