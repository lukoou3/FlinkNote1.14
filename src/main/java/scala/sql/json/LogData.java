package scala.sql.json;

import java.io.Serializable;

public class LogData implements Serializable {
    private String common_schema_type;
    private Long common_recv_time;
    private String common_client_ip;
    private String common_server_ip;
    private String http_host;
    private String http_domain;
    private Long common_vsys_id;
    private String common_device_group;
    private String common_data_center;
    private String common_l4_protocol;
    private String common_internal_ip;
    private String common_external_ip;
    private String common_subscriber_id;
    private Long common_sessions;
    private String common_app_label;
    private Long common_c2s_pkt_num;
    private Long common_s2c_pkt_num;
    private Long common_c2s_byte_num;
    private Long common_s2c_byte_num;

    public String getCommon_schema_type() {
        return common_schema_type;
    }

    public void setCommon_schema_type(String common_schema_type) {
        this.common_schema_type = common_schema_type;
    }

    public Long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(Long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public String getCommon_client_ip() {
        return common_client_ip;
    }

    public void setCommon_client_ip(String common_client_ip) {
        this.common_client_ip = common_client_ip;
    }

    public String getCommon_server_ip() {
        return common_server_ip;
    }

    public void setCommon_server_ip(String common_server_ip) {
        this.common_server_ip = common_server_ip;
    }

    public String getHttp_host() {
        return http_host;
    }

    public void setHttp_host(String http_host) {
        this.http_host = http_host;
    }

    public String getHttp_domain() {
        return http_domain;
    }

    public void setHttp_domain(String http_domain) {
        this.http_domain = http_domain;
    }

    public Long getCommon_vsys_id() {
        return common_vsys_id;
    }

    public void setCommon_vsys_id(Long common_vsys_id) {
        this.common_vsys_id = common_vsys_id;
    }

    public String getCommon_device_group() {
        return common_device_group;
    }

    public void setCommon_device_group(String common_device_group) {
        this.common_device_group = common_device_group;
    }

    public String getCommon_data_center() {
        return common_data_center;
    }

    public void setCommon_data_center(String common_data_center) {
        this.common_data_center = common_data_center;
    }

    public String getCommon_l4_protocol() {
        return common_l4_protocol;
    }

    public void setCommon_l4_protocol(String common_l4_protocol) {
        this.common_l4_protocol = common_l4_protocol;
    }

    public String getCommon_internal_ip() {
        return common_internal_ip;
    }

    public void setCommon_internal_ip(String common_internal_ip) {
        this.common_internal_ip = common_internal_ip;
    }

    public String getCommon_external_ip() {
        return common_external_ip;
    }

    public void setCommon_external_ip(String common_external_ip) {
        this.common_external_ip = common_external_ip;
    }

    public String getCommon_subscriber_id() {
        return common_subscriber_id;
    }

    public void setCommon_subscriber_id(String common_subscriber_id) {
        this.common_subscriber_id = common_subscriber_id;
    }

    public Long getCommon_sessions() {
        return common_sessions;
    }

    public void setCommon_sessions(Long common_sessions) {
        this.common_sessions = common_sessions;
    }

    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
    }

    public Long getCommon_c2s_pkt_num() {
        return common_c2s_pkt_num;
    }

    public void setCommon_c2s_pkt_num(Long common_c2s_pkt_num) {
        this.common_c2s_pkt_num = common_c2s_pkt_num;
    }

    public Long getCommon_s2c_pkt_num() {
        return common_s2c_pkt_num;
    }

    public void setCommon_s2c_pkt_num(Long common_s2c_pkt_num) {
        this.common_s2c_pkt_num = common_s2c_pkt_num;
    }

    public Long getCommon_c2s_byte_num() {
        return common_c2s_byte_num;
    }

    public void setCommon_c2s_byte_num(Long common_c2s_byte_num) {
        this.common_c2s_byte_num = common_c2s_byte_num;
    }

    public Long getCommon_s2c_byte_num() {
        return common_s2c_byte_num;
    }

    public void setCommon_s2c_byte_num(Long common_s2c_byte_num) {
        this.common_s2c_byte_num = common_s2c_byte_num;
    }

    @Override
    public String toString() {
        return "LogData{" +
                "common_schema_type='" + common_schema_type + '\'' +
                ", common_recv_time='" + common_recv_time + '\'' +
                ", common_client_ip='" + common_client_ip + '\'' +
                ", common_server_ip='" + common_server_ip + '\'' +
                ", http_host='" + http_host + '\'' +
                ", http_domain='" + http_domain + '\'' +
                ", common_vsys_id=" + common_vsys_id +
                ", common_device_group='" + common_device_group + '\'' +
                ", common_data_center='" + common_data_center + '\'' +
                ", common_l4_protocol='" + common_l4_protocol + '\'' +
                ", common_internal_ip='" + common_internal_ip + '\'' +
                ", common_external_ip='" + common_external_ip + '\'' +
                ", common_subscriber_id='" + common_subscriber_id + '\'' +
                ", common_sessions=" + common_sessions +
                ", common_app_label='" + common_app_label + '\'' +
                ", common_c2s_pkt_num=" + common_c2s_pkt_num +
                ", common_s2c_pkt_num=" + common_s2c_pkt_num +
                ", common_c2s_byte_num=" + common_c2s_byte_num +
                ", common_s2c_byte_num=" + common_s2c_byte_num +
                '}';
    }
}
