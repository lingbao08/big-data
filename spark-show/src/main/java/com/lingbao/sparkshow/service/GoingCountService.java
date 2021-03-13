package com.lingbao.sparkshow.service;

import com.lingbao.entity.EchartsPie;
import com.lingbao.entity.GoingCount;
import com.lingbao.sparkshow.dao.GoingCountDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lingbao08
 * @DESCRIPTION
 * @create 2019-10-23 19:20
 **/
@Service
@Slf4j
public class GoingCountService {

    @Autowired
    private GoingCountDao goingCountDao;

    public List<EchartsPie> getOneDayGoing(String day) {
        try {
            List<GoingCount> goingCounts = goingCountDao.preRowKeyCount(day);
            for (GoingCount goingCount : goingCounts) {
                log.info(goingCount.toString());
            }
            return goingCounts.parallelStream().map(goingCount ->
                    EchartsPie.builder()
                            .name(goingCount.getDay_desc().split("_")[1])
                            .value(goingCount.getGo_count()).build()
            ).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }
}
