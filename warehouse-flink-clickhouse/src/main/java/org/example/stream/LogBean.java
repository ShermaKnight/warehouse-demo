package org.example.stream;

import lombok.Data;

import java.util.List;

@Data
public class LogBean {

    private CommonBean common;
    private PageBean page;
    private long ts;
    private List<DisplaysBean> displays;

    @Data
    public static class CommonBean {
        private String ar;
        private String ba;
        private String ch;
        private String is_new;
        private String md;
        private String mid;
        private String os;
        private String uid;
        private String vc;
    }

    @Data
    public static class PageBean {
        private int during_time;
        private String page_id;
    }

    @Data
    public static class DisplaysBean {
        private String display_type;
        private String item;
        private String item_type;
        private int order;
        private int pos_id;
    }
}
