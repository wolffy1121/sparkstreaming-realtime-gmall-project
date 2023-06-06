package com.wolffy.sparkstreaming.realtime.bean

case class PageLog(
                    mid: String,
                    user_id: String,
                    province_id: String,
                    channel: String,
                    is_new: String,
                    model: String,
                    operate_system: String,
                    version_code: String,


                    page_id: String,
                    last_page_id: String,
                    page_item: String,
                    page_item_type: String,
                    during_time: Long,

                    ts: Long
                  ) {

}
/**
 * "common":{
 * "ar":"4",
 * "ba":"iPhone",
 * "ch":"Appstore",
 * "is_new":"1",
 * "md":"iPhone Xs Max",
 * "mid":"mid_185",
 * "os":"iOS 13.3.1",
 * "uid":"45",
 * "vc":"v2.1.132"
 * },
 */

/**
 * "page":{
 * "during_time":18904,
 * "item":"电视",
 * "item_type":"keyword",
 * "last_page_id":"home",
 * "page_id":"good_list"
 * },
 */