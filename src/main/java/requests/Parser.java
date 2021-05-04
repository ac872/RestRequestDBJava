package requests;

import java.util.ArrayList;

public class Parser {
    private ArrayList<Message> messages = new ArrayList<>();

    public ArrayList<Message> getMessages() { return messages; }
}

class Message {
    private long msg_id;
    private String company_name;
    private String registration_date;
    private float score;
    private int directors_count;
    private String last_updated;

    public long getMsg_id() {
        return msg_id;
    }

    public String getCompany_name() {
        return company_name;
    }

    public String getReg_date() {
        return registration_date;
    }

    public float getScore() {
        return score;
    }

    public int getDir_count() {
        return directors_count;
    }

    public String getLast_updated() {
        return last_updated;
    }
}