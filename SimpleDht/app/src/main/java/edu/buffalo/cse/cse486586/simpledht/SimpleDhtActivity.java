package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {


    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        final Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));


        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Cursor cur = getContentResolver().query(mUri, null,"@",null,null);
                tv.setText("");
                String s = "";
                int k = cur.getColumnIndex("key");
                int val = cur.getColumnIndex("value");

                if(cur.moveToFirst()){
                    do {
                        s +=cur.getString(k)+":"+cur.getString(val);
                        s+="\n";
                    }while (cur.moveToNext());
                }

                tv.setText(s);
            }
        });

        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Cursor cur = getContentResolver().query(mUri,null,"*",null,null);
                tv.setText("");
                String s = "";
                int k = cur.getColumnIndex("key");
                int val = cur.getColumnIndex("value");

                if(cur.moveToFirst()){
                    do {
                        s +=cur.getString(k)+":"+cur.getString(val);
                        s+="\n";
                    }while (cur.moveToNext());
                }

                tv.setText(s);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
