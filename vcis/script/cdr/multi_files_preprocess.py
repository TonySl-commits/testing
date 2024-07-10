from vcis.cdr.multi_file_format.integration import CDR_Integration

integration = CDR_Integration(passed_connection_host = "10.1.10.66")

formated_in_calls, formated_out_calls = integration.intergrate_call()
print(formated_out_calls.show(5))
formated_in_sms, formated_out_sms = integration.intergrate_sms()
print(formated_out_sms.show(5))
formated_voltein, formated_volteout = integration.intergrate_volte()
print(formated_voltein.show(5))
formated_data_session = integration.intergrate_data_session()
print(formated_data_session.show(5))