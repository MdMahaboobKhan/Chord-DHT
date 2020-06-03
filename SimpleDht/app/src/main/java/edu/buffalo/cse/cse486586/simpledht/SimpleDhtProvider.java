package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.widget.EditText;
import android.widget.TextView;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String NODE1 = "5554";
    static final String[] nodes = {"5554","5556","5558","5560","5562"};
    private final Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");

    private static HashMap<String,String> nodemap = new HashMap<String, String>();
    private ConcurrentHashMap<String,String> mssgMap;
    private ConcurrentHashMap<String, String> Qmap;
    private String portStr = null;
    private String nodeID = null;
    private String predID = null;
    private String predPort = null;
    private String succPort = null;
    public static boolean wait = true;

//    public static int count = 0;

    private final String JOIN_REQ = "J";
    private final String INS_TAG = "I";
    private final String Q_TAG = "Q";
    private final String Q_RESP = "QR";
    private final String DEL_TAG = "D";
    private final String SUCC_UPDATE = "S";
    private final String PRED_UPDATE = "P";
    private final String ALL_PAIRS = "*";
    private final String LOCAL_PAIRS = "@";

//    private String PORT1 = null;
    private Uri buildUri(String scheme, String authority) {
    Uri.Builder uriBuilder = new Uri.Builder();
    uriBuilder.authority(authority);
    uriBuilder.scheme(scheme);
    return uriBuilder.build();
}

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        String msg = null;
        int delFiles = 0;
        Context ctx = getContext();
        if (selection.equals(LOCAL_PAIRS)){
            String[] files = ctx.fileList();
            try {
                for(String f : files){
                    ctx.deleteFile(f);
                    delFiles+=1;
                }
            }catch (Exception e){
                //file is already deleted
            }

            return delFiles;
        }
        else if (selection.equals(ALL_PAIRS)){
            String[] files = ctx.fileList();
            for(String f : files){
                try {
                    ctx.deleteFile(f);
                    delFiles+=1;
                }catch (Exception e){
                    Log.e(TAG,"File already deleted");
                }

            }
            for (String i: mssgMap.keySet()){
                mssgMap.remove(i);
            }
            msg = DEL_TAG+":"+portStr+":"+ALL_PAIRS;
            sendMsgToClient(succPort,msg);
            long sTime = System.currentTimeMillis();
            while ((System.currentTimeMillis()-sTime)<1000){
                //just wait
            }
            return delFiles;
        }
        else {
            if (partOfSameNode(selection)){
                try {
                    ctx.deleteFile(selection);
                    mssgMap.remove(selection);
                    delFiles+=1;
                }catch (Exception e){
                    Log.e(TAG,"File already deleted");

                }

                return delFiles;
            }
            else {
                msg = DEL_TAG+":"+portStr+":"+selection;
                sendMsgToClient(succPort,msg);

                long sTime = System.currentTimeMillis();
                while ((System.currentTimeMillis()-sTime)<300){
                    //just wait
                }
            }
        }

        return delFiles;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        FileOutputStream outputStream;
        Context ctx = getContext();
        if (partOfSameNode(key)){
            try {
                outputStream = ctx.openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();

            }catch (Exception e){
            Log.e(TAG,"INSERTION ERROR",e);
            }
        }
        else {
            //send to successor
            String ins_msg = INS_TAG+":"+portStr+":"+key+";"+value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,ins_msg,succPort);

        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        //String node_id = genHash("5558");
        /* From PA2*/
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
//        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
//        final String PORT1 = String.valueOf((Integer.parseInt(NODE1)*2));



        for (String i:nodes){
            String p = String.valueOf((Integer.parseInt(i) * 2));
            nodemap.put(i,p);
        }

        mssgMap = new ConcurrentHashMap<String, String>();
        Qmap = new ConcurrentHashMap<String, String>();
//        count +=1;
//
//        Log.e(TAG,count+"  ----> "+portStr);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        try {
            nodeID = genHash(portStr);

        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"nodeID ERROR :",e);
        }

        if (!portStr.equals(NODE1)){
            // Since all node join requests happen at NODE1, if this isn't NODE1, send join request to NODE1

            String msg = JOIN_REQ+":"+portStr+":"+portStr;

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,nodemap.get(NODE1));
        }

        return false;
    }



    private boolean partOfSameNode(String k){
        String hash = null;

        if(predID == null && succPort == null){
            return true;
        }

        try{
            hash = genHash(k);

        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG,"SAME NODE Check ERROR",e);
        }

        if (hash!=null){

            if (predID.compareTo(hash)<0){
                if(hash.compareTo(nodeID)<=0){
                    return true;
                }
            }

            if (predID.compareTo(nodeID)>0){
                // checking the last partition
                if (predID.compareTo(hash)<0 || hash.compareTo(nodeID)<=0){
                    return true;
                }
            }
        }

        return false;
    }

    public int getCase(String str){

        if (str.equals(JOIN_REQ))
            return 0;

        else if (str.equals(PRED_UPDATE))
            return 1;
        else if (str.equals(SUCC_UPDATE))
            return 2;
        else if (str.equals(INS_TAG))
            return 3;
        else if (str.equals(Q_TAG))
            return 4;
        else if (str.equals(Q_RESP))
            return 5;
        else if (str.equals(DEL_TAG))
            return 6;

        return 7;
    }


    public void sendMsgToClient(String msg, String dest){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, dest);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        String msg = null;

        if (selection.equals(LOCAL_PAIRS)){

            FileInputStream ipstream;
            Context ctx = getContext();
            String val = "";

            MatrixCursor cur = new MatrixCursor(new String[]{"key","value"});

            try{
                String[] files = ctx.fileList();
                for(String i:files) {
                    ipstream = ctx.openFileInput(i);
                    InputStreamReader ip = new InputStreamReader(ipstream);
                    BufferedReader br = new BufferedReader(ip);
                    val = br.readLine();
                    cur.newRow().add("key",i).add("value",val);
                    ipstream.close();
                }
            }catch (Exception e){
                Log.e(TAG,"File read failed in @ segment");
            }

            return cur;
        }
        else if (selection.equals(ALL_PAIRS)){
            // do LOCAL_PAIRS for all AVDs
            FileInputStream ipstream;
            Context ctx = getContext();
//            String val = "";
            String s = "";
            MatrixCursor cur = new MatrixCursor(new String[]{"key","value"});

            try{
                String[] files = ctx.fileList();
                if (files.length!=0) {
                    for (String i : files) {
                        String val = "";
                        ipstream = ctx.openFileInput(i);
                        InputStreamReader ip = new InputStreamReader(ipstream);
                        BufferedReader br = new BufferedReader(ip);
                        val = br.readLine();

                        if (i.equals("") || val.equals("")) {
                            ipstream.close();
                            continue;
                        }

                        s += i + ";" + val + "/";
                        ipstream.close();
                    }
                }

            }catch (Exception e){
                Log.e(TAG,"File read failed in @ segment of *");
            }



            if (succPort!=null){
                String msg1 = Q_TAG+":"+portStr+":"+ALL_PAIRS+":"+s;
                long startTime = System.currentTimeMillis();
                sendMsgToClient(msg1,succPort);
                while ((System.currentTimeMillis()-startTime)<1500){
                    //wait
                }

            }else {
                if (s.length()!=0){
                    s = s.substring(0,s.length()-1);
                    String[] str = s.split("/");
                    for (String i: str) {
                        String[] x = i.split(";");
                        mssgMap.put(x[0], x[1]);
                    }

                }
            }

            Log.e(TAG,"end of wait");

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (mssgMap.size()!=0){
                for (String k: mssgMap.keySet()){
                    cur.newRow().add("key",k).add("value",mssgMap.get(k));
                }
            }



            return cur;

        }
        else {
            // if selection is not @ or *
            if (partOfSameNode(selection)){

                FileInputStream ipstream;
                Context ctx = getContext();
                String val = "";

                MatrixCursor cur = new MatrixCursor(new String[]{"key","value"});

                try {
                    ipstream = ctx.openFileInput(selection);
                    InputStreamReader ip = new InputStreamReader(ipstream);
                    BufferedReader br = new BufferedReader(ip);
                    val = br.readLine();
                    cur.newRow().add("key",selection).add("value",val);
                    ipstream.close();

                }catch (Exception e){
                    Log.e(TAG,"File read failed in single query section",e);
                }

                return cur;
            }
            else {
                // not part of same partition

                if (succPort != null) {
                    String val = "";
                    msg = Q_TAG + ":" + portStr + ":" + selection;
                    Qmap.put(selection,val);
                    sendMsgToClient(msg, succPort);
                    Log.e(TAG,"msg sent to succ"+succPort);

                    MatrixCursor cur = new MatrixCursor(new String[]{"key","value"});

                    my_synchronizer();

                    cur.newRow().add("key",selection).add("value",Qmap.get(selection));

                    return cur;

                }
            }
        }


        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public void my_synchronizer()
    {
        wait = true;
        while(wait);
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];


            Socket socket = null;

            try{
                while (true) {
                    socket = serverSocket.accept();
//                    Log.e(TAG, "Connection successful");
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String msg = br.readLine();

//                    Log.e(TAG, "Message Read");
                    if (msg != null) {
                        String[] chunk = msg.split(":");
                        String tag = chunk[0];
                        String src = chunk[1];


                        if (!src.equals(portStr)){
                            String msgContent = chunk[2];

                            int ch = getCase(tag);

                            switch (ch){
                                case 0:
                                    if(predID == null && succPort == null){
                                        //update successor and predecessor

                                        succPort = nodemap.get(msgContent);
                                        String succMsgUpdate = SUCC_UPDATE+":"+portStr+":"+portStr;
                                        sendMsgToClient(succMsgUpdate,succPort);

                                        String predMsgUpdate = PRED_UPDATE+":"+portStr+":"+portStr;
                                        sendMsgToClient(predMsgUpdate,succPort);
                                    }else {
                                        if (partOfSameNode(msgContent)){

                                            String succMsgUpdate = SUCC_UPDATE+":"+portStr+":"+msgContent;
                                            sendMsgToClient(succMsgUpdate,predPort);

                                            succMsgUpdate = SUCC_UPDATE + ":" + portStr + ":" + portStr;
                                            sendMsgToClient(succMsgUpdate,nodemap.get(msgContent));

                                            predID = genHash(msgContent);
                                            predPort = nodemap.get(msgContent);
                                        }else {
                                            sendMsgToClient(msg,succPort);
                                        }
                                    }
                                    break;


                                case 1:
                                    predID = genHash(msgContent);
                                    predPort = nodemap.get(msgContent);
                                    break;


                                case 2:
                                    succPort = nodemap.get(msgContent);

                                    String predMsgUpdate = PRED_UPDATE+":"+portStr+":"+portStr;
                                    sendMsgToClient(predMsgUpdate,succPort);
                                    break;



                                case 3:
                                    String[] arr = msgContent.split(";");
                                    ContentValues keyValToInsert = new ContentValues();
                                    if (partOfSameNode(arr[0])){
                                        try {
                                            keyValToInsert.put("key",arr[0]);
                                            keyValToInsert.put("value",arr[1]);
                                            insert(null,keyValToInsert);

                                        } catch (Exception e) {
                                            Log.e(TAG, "File write failed");
                                        }
                                    }
                                    else {
                                        sendMsgToClient(msg,succPort);
                                    }
                                    break;

                                case 4:
                                    if (msgContent.equals(ALL_PAIRS)) {
                                        String s = "";
                                        FileInputStream ipstream1;
                                        Context ctx = getContext();
                                        try {
                                            s = chunk[3];
                                        }catch (Exception e){
                                            s = "";
                                        }


                                        try{
                                            String[] files = ctx.fileList();
                                            if (files.length!=0){
                                                for(String i:files) {
                                                    String val = "";
                                                    ipstream1 = ctx.openFileInput(i);
                                                    InputStreamReader ip1 = new InputStreamReader(ipstream1);
                                                    BufferedReader b = new BufferedReader(ip1);
                                                    val = b.readLine();
                                                    if (i.equals("") || val.equals("")){
                                                        ipstream1.close();
                                                        continue;
                                                    }
                                                    s+=i+";"+val+"/";
                                                    ipstream1.close();
                                                }
                                            }

                                        }catch (Exception e){
                                            Log.e(TAG,"File read failed in @ segment of *");
                                        }



                                        if ((nodemap.get(src)).equals(succPort)){
                                            // if last node
                                            Log.e(TAG,"LAST NODE"+" source = "+nodemap.get(src)+"  successor = "+succPort);
                                            Log.e(TAG,"source = "+nodemap.get(src));
                                            Log.e(TAG,"successor = "+succPort);
                                            if (s.length()!=0){
                                                int l = s.length();
                                                if (s.charAt(l-1)=='/'){
                                                    s = s.substring(0,s.length()-1);
                                                }
                                            }
                                            String m2 = Q_RESP+":"+portStr+":"+msgContent+":"+s;
                                            sendMsgToClient(m2,nodemap.get(src));

                                        }else{
                                            Log.e(TAG,"AVD "+portStr);
                                            // intermediate nodes just forward request along with local contents
                                            String m = Q_TAG+":"+src+":"+msgContent+":"+s;
                                            sendMsgToClient(m, succPort);
                                            Log.e(TAG,"NOT LAST NODE"+" source = "+nodemap.get(src)+"  successor = "+succPort+"  predecessor = "+predPort);

                                        }


                                    } else {
                                        if (partOfSameNode(msgContent)) {

                                            Cursor c = query(null, null, msgContent, null, null);
                                            int a = c.getColumnIndex("key");
                                            int b = c.getColumnIndex("value");

                                            if (c.moveToFirst()){
                                                String m = Q_RESP + ":" + portStr + ":" + msgContent + ":" + c.getString(b);
                                                sendMsgToClient(m, nodemap.get(src));
                                            }
//                                        Log.e(TAG, "message sent back to pred" + nodemap.get(src)+" AVD "+src+" from "+portStr);
                                            c.close();
                                        } else {

                                            sendMsgToClient(msg, succPort);
//                                        Log.e(TAG, "message sent to successor" + succPort);
                                        }
                                    }
                                    break;


                                case 5:
                                    if (msgContent.equals(ALL_PAIRS)){
                                        try {
                                            String[] resp_pairs = chunk[3].split("/");
                                            for (String i : resp_pairs) {
//                                        Log.e(TAG,i);
                                                if (i.equals(";") || i.length() == 0)
                                                    continue;
                                                String[] arr1 = i.split(";");
                                                mssgMap.put(arr1[0], arr1[1]);
                                            }
                                        }catch (Exception e){
                                            Log.e(TAG,"No elements");
                                        }

//                                        inner_wait = false;
//                                        Log.e(TAG,"inner_sync ends");

//                                    }

                                    }else {
                                        Qmap.put(msgContent,chunk[3]);
                                        wait = false;

                                    }
                                    break;


                                case 6:
                                    if (msgContent.equals(ALL_PAIRS)){
                                        delete(null,LOCAL_PAIRS,null);
                                        for (String i: mssgMap.keySet()){
                                            mssgMap.remove(i);
                                        }
                                        sendMsgToClient(msg,succPort);
                                    }
                                    else {
                                        if (partOfSameNode(msgContent)){

                                            delete(null,msgContent,null);
                                        }
                                        else {
                                            sendMsgToClient(succPort,msg);
                                        }
                                    }
                                    break;

                            }

                        }

                    }

                }
            }catch (Exception ex){
                Log.e(TAG,"SERVER ERROR",ex);
            }

            return null;
        }

    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {



            Socket socket = null;
            try {
                String remotePort = msgs[1];

                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                String msgToSend = msgs[0];
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                try {
                    PrintWriter pw = new PrintWriter(socket.getOutputStream(),true);
//                    Log.e(TAG,msgToSend+" ---> at CLIENT");
                    pw.println(msgToSend);
//                    Log.e(TAG, "client message sent");


                } catch (Exception e) {
                    Log.e(TAG, "CLIENT ERROR",e);

                }



            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;

        }
    }




}
