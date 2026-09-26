/*
  Android_Pebble_sd - Android alarm client for openseizuredetector..

  See http://openseizuredetector.org for more information.

  Copyright Graham Jones, 2015, 2016

  This file is part of pebble_sd.

  Android_Pebble_sd is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  Android_Pebble_sd is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Android_pebble_sd.  If not, see <http://www.gnu.org/licenses/>.

*/
package uk.org.openseizuredetector.datasource;
import uk.org.openseizuredetector.R;

import uk.org.openseizuredetector.SdServer;
import uk.org.openseizuredetector.datasource.SdDataSource;
import uk.org.openseizuredetector.datasource.SdDataSourceAw;
import android.content.Context;
import android.os.Handler;
import androidx.preference.PreferenceManager;
import uk.org.openseizuredetector.data.logging.Log;

import com.google.android.gms.tasks.Task;
import com.google.android.gms.tasks.TaskCompletionSource;
import com.google.android.gms.tasks.Tasks;
import com.google.android.gms.wearable.MessageClient;
import com.google.android.gms.wearable.MessageEvent;
import com.google.android.gms.wearable.Wearable;

import org.json.JSONException;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


/**
 * Android Wear data source that receives accelerometer and heart rate data from a
 * companion watch app via the Wearable Data Layer API.
 *
 * This implementation expects the watch app to send messages on the following paths:
 * - "/osd/accel_data" - Accelerometer data (raw samples)
 * - "/osd/settings" - Watch settings and battery status
 * - "/osd/hr_data" - Heart rate measurements
 */
public class SdDataSourceAw extends SdDataSource implements MessageClient.OnMessageReceivedListener {
    private String TAG = "SdDataSourceAw";
    private static final long LATENCY_WARN_MS = 30_000;
    private static final long LATENCY_CRITICAL_MS = 120_000;

    // Message paths for Wearable Data Layer communication
    private static final String PATH_ACCEL_DATA = "/osd/accel_data";
    private static final String PATH_SETTINGS = "/osd/settings";
    private static final String PATH_HR_DATA = "/osd/hr_data";
    private static final String PATH_REQUEST_DATA = "/osd/request_data";
    private static final String PATH_ALARM_STATE = "/osd/alarm_state";
    private static final String PATH_SEND_SETTINGS = "/osd/send_settings";
    private static final String PATH_USER_ACTION = "/osd/user_action";

    // Raw data storage
    private int MAX_RAW_DATA = 125;  // 5 seconds at 25 Hz
    private double[] rawData = new double[MAX_RAW_DATA];
    private int nRawData = 0;
    private long mCurrentAccelSeq = -1;
    private long mCurrentAccelSentMs = -1;
    private long mCurrentAccelReceivedMs = 0;
    private long mLastProcessedAccelSeq = -1;
    private long mLastProcessedAccelSentMs = -1;
    private long mLastProcessedAccelReceivedMs = 0;

    private MessageClient mMessageClient;
    private boolean mIsStarted = false;
    private final Executor mExecutor = Executors.newSingleThreadExecutor();

    public SdDataSourceAw(Context context, Handler handler,
                          SdDataReceiver sdDataReceiver) {
        super(context, handler, sdDataReceiver);
        mName = "Android Wear";
        // REMOVED redundant setDefaultValues() - now centralized in PrefActivity.initialiseDefaultValues()
    }


    /**
     * Start the datasource updating - initialises from sharedpreferences first to
     * make sure any changes to preferences are taken into account.
     */
    @Override
    public void start() {
        Log.i(TAG, "start()");
        super.start();

        // Register message listener
        mMessageClient = Wearable.getMessageClient(mContext);
        mMessageClient.addListener(this);
        mIsStarted = true;

        mSdData.watchConnected = true;
        mSdData.watchAppRunning = false;

        // Request initial data from watch
        sendMessageToWatch(PATH_SEND_SETTINGS, "start".getBytes(StandardCharsets.UTF_8));

        Log.v(TAG, "start(): Android Wear message listener registered");
    }

    /**
     * Stop the datasource from updating
     */
    @Override
    public void stop() {
        Log.v(TAG, "stop()");

        try {
            if (mMessageClient != null && mIsStarted) {
                mMessageClient.removeListener(this);
                mIsStarted = false;
                Log.v(TAG, "stop(): message listener removed");
                Log.i(TAG, "SdDataSourceAw.stop() - message listener removed");
            }
        } catch (Exception e) {
            Log.v(TAG, "Error in stop() - " + e.toString());
        }

        super.stop();
    }

    /**
     * Called when a message is received from the watch
     */
    @Override
    public void onMessageReceived(MessageEvent messageEvent) {
        String path = messageEvent.getPath();
        byte[] data = messageEvent.getData();
        long receivedMs = System.currentTimeMillis();

        Log.v(TAG, "onMessageReceived: " + path);

        // Mark that we've received data from the watch
        mWatchAppRunningCheck = true;
        mSdData.watchConnected = true;
        mSdData.watchAppRunning = true;
        mDataStatusTimeMillis = receivedMs;
        mSdData.watchLastPayloadPath = path;
        mSdData.watchLastPayloadReceivedMs = receivedMs;

        try {
            if (path.equals(PATH_ACCEL_DATA)) {
                handleAccelData(data, receivedMs);
            } else if (path.equals(PATH_SETTINGS)) {
                handleSettings(data);
            } else if (path.equals(PATH_HR_DATA)) {
                handleHrData(data);
            } else if (path.equals(PATH_USER_ACTION)) {
                handleUserAction(data);
            } else {
                Log.w(TAG, "Unknown message path: " + path);
            }
        } catch (Exception e) {
            Log.e(TAG, "Error processing message: " + e.toString());
        }
    }

    /**
     * Handle accelerometer data from watch
     * Expected format: JSON with "samples" array or raw binary data
     */
    private void handleAccelData(byte[] data, long receivedMs) {
        try {
            // Try to parse as JSON first
            String jsonStr = new String(data, StandardCharsets.UTF_8);
            JSONObject json = new JSONObject(jsonStr);
            long seq = json.optLong("seq", -1);
            long sentMs = json.optLong("sent_ms", -1);
            recordAccelPayloadTiming(seq, sentMs, receivedMs);

            if (json.has("samples")) {
                // JSON format with samples array
                org.json.JSONArray samples = json.getJSONArray("samples");
                for (int i = 0; i < samples.length(); i++) {
                    appendAccelSample(samples.getDouble(i), seq, sentMs, receivedMs);
                }
            } else if (json.has("x") && json.has("y") && json.has("z")) {
                // Single 3D sample
                double x = json.getDouble("x");
                double y = json.getDouble("y");
                double z = json.getDouble("z");
                double magnitude = Math.sqrt(x * x + y * y + z * z);
                appendAccelSample(magnitude, seq, sentMs, receivedMs);
            }
        } catch (JSONException e) {
            // Not JSON, try parsing as binary data
            try {
                parseBinaryAccelData(data);
            } catch (Exception ex) {
                Log.e(TAG, "Error parsing accel data: " + ex.toString());
            }
        }
    }

    private void recordAccelPayloadTiming(long seq, long sentMs, long receivedMs) {
        mSdData.watchLastAccelSeq = seq;
        mSdData.watchLastAccelSentMs = sentMs;
        mSdData.watchLastAccelReceivedMs = receivedMs;
        mSdData.watchLastAccelLatencyMs = sentMs > 0 ? receivedMs - sentMs : -1;

        if (mSdData.watchLastAccelLatencyMs < 0) {
            Log.i(TAG, "rxTiming path=" + PATH_ACCEL_DATA + " seq=" + seq
                    + " receivedMs=" + receivedMs + " latencyMs=unknown");
            return;
        }

        if (mSdData.watchLastAccelLatencyMs > mSdData.watchWorstAccelLatencyMs) {
            mSdData.watchWorstAccelSeq = seq;
            mSdData.watchWorstAccelSentMs = sentMs;
            mSdData.watchWorstAccelReceivedMs = receivedMs;
            mSdData.watchWorstAccelLatencyMs = mSdData.watchLastAccelLatencyMs;
        }

        String message = "rxTiming path=" + PATH_ACCEL_DATA
                + " seq=" + seq
                + " sentMs=" + sentMs
                + " receivedMs=" + receivedMs
                + " latencyMs=" + mSdData.watchLastAccelLatencyMs;
        if (mSdData.watchLastAccelLatencyMs >= LATENCY_CRITICAL_MS) {
            Log.e(TAG, "timingCritical " + message);
        } else if (mSdData.watchLastAccelLatencyMs >= LATENCY_WARN_MS) {
            Log.w(TAG, "timingWarn " + message);
        } else {
            Log.i(TAG, "timingOk " + message);
        }
    }

    /**
     * Parse binary accelerometer data (16-bit little-endian shorts)
     */
    private void parseBinaryAccelData(byte[] data) {
        // Assume 16-bit little-endian signed integers
        short[] samples = new short[data.length / 2];
        ByteBuffer.wrap(data)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asShortBuffer()
                .get(samples);

        for (short sample : samples) {
            appendAccelSample(sample, -1, -1, System.currentTimeMillis());
        }
    }

    private void appendAccelSample(double sample, long seq, long sentMs, long receivedMs) {
        if (nRawData == 0) {
            mCurrentAccelSeq = seq;
            mCurrentAccelSentMs = sentMs;
            mCurrentAccelReceivedMs = receivedMs;
        }

        rawData[nRawData] = sample;
        nRawData++;

        if (nRawData >= MAX_RAW_DATA) {
            processAccelData(mCurrentAccelSeq, mCurrentAccelSentMs, mCurrentAccelReceivedMs);
            nRawData = 0;
            mCurrentAccelSeq = -1;
            mCurrentAccelSentMs = -1;
            mCurrentAccelReceivedMs = 0;
        }
    }

    /**
     * Process buffered accelerometer data by calling doAnalysis
     */
    private void processAccelData(long accelSeq, long accelSentMs, long accelReceivedMs) {
        Log.v(TAG, "processAccelData(): processing " + nRawData + " samples");
        mLastProcessedAccelSeq = accelSeq;
        mLastProcessedAccelSentMs = accelSentMs;
        mLastProcessedAccelReceivedMs = accelReceivedMs;

        // Copy to mSdData
        for (int i = 0; i < nRawData && i < mSdData.rawData.length; i++) {
            mSdData.rawData[i] = rawData[i];
        }
        mSdData.mNsamp = Math.min(nRawData, mSdData.rawData.length);

        // Run analysis
        doAnalysis();

        // Send alarm state back to watch
        sendAlarmStateToWatch();
    }

    /**
     * Handle settings data from watch (battery, version, etc.)
     */
    private void handleSettings(byte[] data) {
        try {
            String jsonStr = new String(data, StandardCharsets.UTF_8);
            JSONObject json = new JSONObject(jsonStr);

            if (json.has("battery")) {
                mSdData.batteryPc = json.getInt("battery");
            }
            if (json.has("version")) {
                mSdData.watchSdVersion = json.getString("version");
            }
            if (json.has("name")) {
                mSdData.watchSdName = json.getString("name");
            }
            if (json.has("sample_freq")) {
                mSdData.mSampleFreq = json.getInt("sample_freq");
                Log.v(TAG, "Watch sample frequency: " + mSdData.mSampleFreq);
            } else {
                mSdData.mSampleFreq = 25;  // default if not provided by watch
            }

            mSdData.haveSettings = true;

            Log.v(TAG, "handleSettings(): battery=" + mSdData.batteryPc +
                    ", version=" + mSdData.watchSdVersion);

        } catch (JSONException e) {
            Log.e(TAG, "Error parsing settings: " + e.toString());
        }
    }

    /**
     * Handle heart rate data from watch
     */
    private void handleHrData(byte[] data) {
        try {
            String jsonStr = new String(data, StandardCharsets.UTF_8);
            JSONObject json = new JSONObject(jsonStr);

            if (json.has("hr")) {
                mSdData.mHR = json.getInt("hr");
                Log.v(TAG, "handleHrData(): HR=" + mSdData.mHR);
            }
        } catch (JSONException e) {
            // Try parsing as simple integer
            try {
                String hrStr = new String(data, StandardCharsets.UTF_8);
                mSdData.mHR = Integer.parseInt(hrStr.trim());
                Log.v(TAG, "handleHrData(): HR=" + mSdData.mHR);
            } catch (Exception ex) {
                Log.e(TAG, "Error parsing HR data: " + ex.toString());
            }
        }
    }

    private void handleUserAction(byte[] data) {
        if (!(mSdDataReceiver instanceof SdServer)) {
            Log.w(TAG, "handleUserAction(): receiver is not SdServer");
            return;
        }

        try {
            String jsonStr = new String(data, StandardCharsets.UTF_8);
            JSONObject json = new JSONObject(jsonStr);
            String action = json.optString("action", "");
            SdServer sdServer = (SdServer) mSdDataReceiver;

            if ("mute".equals(action)) {
                long seconds = json.optLong("seconds", 600);
                Log.i(TAG, "handleUserAction(): mute for " + seconds + " seconds");
                sdServer.muteForSeconds(seconds);
            } else if ("unmute".equals(action)) {
                Log.i(TAG, "handleUserAction(): unmute");
                sdServer.cancelAudibleMute();
            } else if ("accept".equals(action)) {
                Log.i(TAG, "handleUserAction(): accept");
                sdServer.acceptAlarm();
            } else {
                Log.w(TAG, "handleUserAction(): unknown action=" + action);
            }

            sendAlarmStateToWatch();
        } catch (JSONException e) {
            Log.e(TAG, "Error parsing user action: " + e.toString());
        }
    }

    /**
     * Send alarm state back to watch
     */
    private void sendAlarmStateToWatch() {
        try {
            JSONObject json = new JSONObject();
            json.put("alarm_state", mSdData.alarmState);
            json.put("alarm_phrase", mSdData.alarmPhrase);
            json.put("accel_seq", mLastProcessedAccelSeq);
            json.put("accel_sent_ms", mLastProcessedAccelSentMs);
            json.put("phone_received_ms", mLastProcessedAccelReceivedMs);
            json.put("phone_sent_ms", System.currentTimeMillis());
            if (mSdDataReceiver instanceof SdServer) {
                SdServer sdServer = (SdServer) mSdDataReceiver;
                json.put("muted_until_ms", sdServer.cancelAudibleUntilMillis());
                json.put("audible_alarm_enabled", sdServer.isAudibleAlarmEnabled());
                json.put("audible_warning_enabled", sdServer.isAudibleWarningEnabled());
            }

            byte[] data = json.toString().getBytes(StandardCharsets.UTF_8);
            sendMessageToWatch(PATH_ALARM_STATE, data);

        } catch (JSONException e) {
            Log.e(TAG, "Error creating alarm state message: " + e.toString());
        }
    }

    /**
     * Send a message to the watch app
     */
    private void sendMessageToWatch(String path, byte[] data) {
        Log.v(TAG, "sendMessageToWatch: " + path);
        if (mMessageClient == null) {
            Log.w(TAG, "sendMessageToWatch: mMessageClient is null");
            return;
        }

        TaskCompletionSource<Integer> taskCompletionSource = new TaskCompletionSource<>();
        Task<Integer> sendTask = taskCompletionSource.getTask();

        mExecutor.execute(() -> {
            com.google.android.gms.wearable.NodeClient nodeClient =
                    Wearable.getNodeClient(mContext);

            try {
                for (com.google.android.gms.wearable.Node node :
                        Tasks.await(nodeClient.getConnectedNodes())) {
                    Task<Integer> sendMessageTask = mMessageClient.sendMessage(
                            node.getId(), path, data);
                    Tasks.await(sendMessageTask);
                    Log.d(TAG, "Message sent to " + node.getDisplayName() + ": " + path);
                }
                taskCompletionSource.setResult(0);
            } catch (ExecutionException | InterruptedException e) {
                Log.e(TAG, "Error sending message: " + e.toString());
                taskCompletionSource.setException(e);
            }
        });
    }

    /**
     * Check status of watch connection
     * Called periodically by base class timer
     */
    @Override
    public void getStatus() {
        long tnow = System.currentTimeMillis();

        long tdiff = tnow - mDataStatusTimeMillis;
        Log.v(TAG, "getStatus() - mWatchAppRunningCheck=" + mWatchAppRunningCheck +
                ", tdiff=" + tdiff);

        // Check if we've received data recently
        if (!mWatchAppRunningCheck && tdiff > 30000) {  // 30 seconds
            Log.w(TAG, "getStatus() - No data received from watch in 30 seconds");
            mSdData.watchAppRunning = false;

            if (tdiff > 60000) {  // 60 seconds - trigger fault
                Log.w(TAG, "SdDataSourceAw.getStatus() - Watch app not responding");
                mSdDataReceiver.onSdDataFault(mSdData);
            }
        } else {
            mSdData.watchAppRunning = true;
        }

        // Reset check flag
        if (mWatchAppRunningCheck) {
            mWatchAppRunningCheck = false;
            mDataStatusTimeMillis = System.currentTimeMillis();
        }
    }
}
