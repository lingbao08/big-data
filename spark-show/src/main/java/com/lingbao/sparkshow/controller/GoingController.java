package com.lingbao.sparkshow.controller;

import com.lingbao.entity.EchartsPie;
import com.lingbao.sparkshow.service.GoingCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;

/**
 * @author lingbao08
 * @DESCRIPTION
 * @create 2019-10-23 17:45
 **/

@RestController
public class GoingController {

    @Autowired
    private GoingCountService goingCountService;

    /**
     * 根据天数查询当天所有的记录
     *
     * @param day eg:2019-10-10
     * @return
     */
    @GetMapping("/oneDayWithPie/{day}")
    public List<EchartsPie> oneDayWithPie(@PathVariable(value = "day") String day) {
        return goingCountService.getOneDayGoing(day);
    }

    /**
     * 跳转到
     * @return
     */
    @GetMapping("/index")
    public ModelAndView index() {
        ModelAndView view = new ModelAndView("index");
        return view;
    }


}
