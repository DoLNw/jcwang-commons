package com.hmdp.entity;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    private List<?> list;
    private Long minTime; // 注意，这里的T大写，前端的也得大写，不然这里的小写之后，前端接收不到
    private Integer offset;
}
