package org.example.stream;

import lombok.Data;

import java.util.List;

@Data
public class LogBean {

    /**
     * {"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone 13","mid":"mid_368770","os":"iOS 13.3.1","uid":"63","vc":"v2.1.132"},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","order":1,"pos_id":3}],"page":{"during_time":19393,"page_id":"home"},"ts":1605368266000}
     */

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
