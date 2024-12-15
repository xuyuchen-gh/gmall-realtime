package com.bw.gmall.realtime.common.bean;

/**
 * @author xz
 * @date 2024/12/15
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 注册用户数
    Long registerCt;

}