from .callback_api import set_ka_template
from util.mysql_util import insert_sampling,insert_aggregation,insert_suppression,insert_generalization,insert_masking,insert_categorization

from util.mysql_classification_util import get_mysql_connector_pool as get_class_connector_pool
from util.mysql_util import get_mysql_connector_pool as get_mask_connector_pool

class_pool=get_class_connector_pool()
mask_pool=get_mask_connector_pool()
user = 'default'
if __name__ == "__main__":
    #身份证号01
    id1 = insert_masking(mask_pool, 7, 14, '*', 'ka默认配置_身份证号第一层', user)
    id2 = insert_masking(mask_pool, 15, 18, '*', 'ka默认默认配置_身份证号第二层', user)
    id3 = insert_masking(mask_pool, 1, 6, '*', 'ka默认默认配置_身份证号第三层', user)
    rule_map = {
            "1": id1,
            "2": id2,
            "3": id3
    }
    set_ka_template(user,"","ka默认配置_身份证号",3,rule_map)
    #手机号02
    id1 = insert_masking(mask_pool, 4, 7, '*', 'ka默认配置_手机号第一层', user)
    id2 = insert_masking(mask_pool, 8, 11, '*', 'ka默认默认配置_手机号第二层', user)
    id3 = insert_masking(mask_pool, 1, 3, '*', 'ka默认默认配置_手机号第三层', user)
    rule_map = {
            "1": id1,
            "2": id2,
            "3": id3
        }
    set_ka_template(user, "", "ka默认配置_手机号", 3, rule_map)
    #电子邮箱03
    id1 = insert_masking(mask_pool, 1, 1000, '*', 'ka默认配置_电子邮箱第一层', user)
    rule_map = {
            "1": id1
    }
    set_ka_template(user, "", "ka默认配置_电子邮箱", 1, rule_map)
    #地址05
    id1 = insert_masking(mask_pool, 1, 2, '*', 'ka默认配置_地址第一层', user)
    id2 = insert_masking(mask_pool, 3, 1000, '*', 'ka默认默认配置_地址第二层', user)
    rule_map = {
            "1": id1,
            "2": id2
    }
    set_ka_template(user, "", "ka默认配置_地址", 2, rule_map)
    #MAC地址06
    id1 = insert_masking(mask_pool, 7, 21, '*', 'ka默认配置_MAC地址第一层', user)
    id2 = insert_masking(mask_pool, 1, 21, '*', 'ka默认默认配置_MAC地址第二层', user)
    rule_map = {
            "1": id1,
            "2": id2
    }
    set_ka_template(user, "", "ka默认配置_MAC地址", 2, rule_map)
    #车牌号07
    id1 = insert_masking(mask_pool, 3, 11, '*', 'ka默认配置_车牌号第一层', user)
    id2 = insert_masking(mask_pool, 1, 11, '*', 'ka默认默认配置_车牌号第二层', user)
    rule_map = {
            "1": id1,
            "2": id2
    }
    set_ka_template(user, "", "ka默认配置_车牌号", 2, rule_map)
    #专业学位08
    id1 = insert_masking(mask_pool, 1, 1000, '*', 'ka默认配置_专业学位第一层', user)
    rule_map = {
            "1": id1
    }
    set_ka_template(user, "", "ka默认配置_专业学位", 1, rule_map)
    #专业09
    id1 = insert_masking(mask_pool, 1, 1000, '*', 'ka默认配置_专业第一层', user)
    rule_map = {
            "1": id1
    }
    set_ka_template(user, "", "ka默认配置_专业", 1, rule_map)
    #固定电话10
    id1 = insert_masking(mask_pool, 5, 12, '*', 'ka默认配置_固定电话第一层', user)
    id2 = insert_masking(mask_pool, 1, 50, '*', 'ka默认默认配置_固定电话第二层', user)
    rule_map = {
            "1": id1,
            "2": id2
    }
    set_ka_template(user, "", "ka默认配置_固定电话", 2, rule_map)
    #URL11
    id1 = insert_masking(mask_pool, 8, 1000, '*', 'ka默认配置_URL第一层', user)
    id2 = insert_masking(mask_pool, 1, 1000, '*', 'ka默认默认配置_URL第二层', user)
    rule_map = {
            "1": id1,
            "2": id2
    }
    set_ka_template(user, "", "ka默认配置_URL", 2, rule_map)