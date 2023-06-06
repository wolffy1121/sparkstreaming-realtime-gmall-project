package com.wolffy.sparkstreaming.realtime.bean

case class PageActionLog(
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

                          action_id: String,
                          action_item: String,
                          action_item_type: String,
                          action_ts: Long,

                          ts: Long
                        ) {

}
/**
 * "actions":[
 * {
 * "action_id":"get_coupon",
 * "item":"2",
 * "item_type":"coupon_id",
 * "ts":1685909421685
 * }
 */

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