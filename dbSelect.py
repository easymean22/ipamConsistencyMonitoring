import jaydebeapi
import datetime
import schedule
import time
import os



def job():
    connection = jaydebeapi.connect(
        'com.tmax.tibero.jdbc.TbDriver',
        url,
        [username, password],
        jar_path
    )
    cursor = connection.cursor()
    timeStamp = datetime.datetime.now()
    #IP_ADDRESS_ID, NETWORK_ID, IP_ADDRESS, VM_USER, NAT_USER, TIMESTAMP
    #IP_ADDRESS 
    cursor.execute("SELECT IP.IP_ADDRESS_ID, SN.NETWORK_ID, IP.IP_ADDR " +
                "FROM IP_ADDRESS IP " +
                "JOIN SUBNET SN ON IP.SUBNET_ID = SN.SUBNET_ID")
    result = [(row[0], row[1], row[2], None, None, None) for row in cursor.fetchall()]

    # GUEST_INTERFACE
    cursor.execute("SELECT GI.GUEST_INTERFACE_ID, SN.NETWORK_ID, GI.IPV4_ADDR "+
                    "FROM GUEST_INTERFACE GI "+
                    "JOIN SUBNET SN ON GI.SUBNET_ID = SN.SUBNET_ID")
    guestInterfaces = cursor.fetchall()

    for gi in guestInterfaces:
        guestInterfaceId, networkId, ipv4_addr = gi
        found = False
        for r in result:
            if r[2] == ipv4_addr and r[1] == networkId:
                r_index = result.index(r)
                result[r_index] = (r[0], r[1], r[2], f"VM-{guestInterfaceId}", r[4], r[5])
                found = True
                break

        # 일치하는 항목이 없으면 result에 추가
        if not found:
            result.append((0, networkId, ipv4_addr, f"VM-{guestInterfaceId}", None, "ERROR"))


    # NAT
    cursor.execute("SELECT NAT.NAT_ID, VR.NETWORK_ID, NAT.DST_IP FROM NAT "+
                    "JOIN VROUTER VR ON NAT.VROUTER_ID = VR.VROUTER_ID "+
                    "WHERE NAT.CHAIN='VR-INTLB'")
    natRules = cursor.fetchall()

    for nat in natRules:
        natId, networkId, ipv4_addr = nat
        found = False
        for r in result:
            if r[2] == ipv4_addr and r[1] == networkId:
                r_index = result.index(r)
                result[r_index] = (r[0], r[1], r[2], r[3], f"NAT-{natId}", r[5])
                found = True
                break

        # 일치하는 항목이 없으면 result에 추가
        if not found:
            result.append((0, networkId, ipv4_addr, None, f"NAT-{natId}", "ERROR"))

    for r in result:
        if r[3] == None and r[4] == None:
            r_index = result.index(r)
            result[r_index] = (r[0], r[1], r[2], r[3], r[4], "ERROR")

    result = sorted(result, key=lambda x: x[0])
    #cursor.execute("SELECT IP_ADDRESS_ID, IP_ADDRESS, SUBNET_ID FROM IP_ADDRESS")
    format_string = "{:<15} {:<10} {:<15} {:<20} {:<20} {}"

    cursor.close()
    connection.close()
    
    # Compare with previous results if exists
    file_name = "result_previous.txt"
    if os.path.exists(file_name):
        with open(file_name, "r") as file:
            previous_content = file.read()
    else:
        previous_content = ""

    content = []
    content.append(format_string.format("IP_ADDRESS_ID", "NETWORK_ID", "IP_ADDRESS", "VM_USER", "NAT_USER", "STATUS"))
    for row in result:
        content.append(format_string.format(row[0], row[1], row[2], row[3] if row[3] else "-", row[4] if row[4] else "-", row[5] if row[5] else "-"))
    content_str = "\n".join(content)

    # Save only if different from previous results
    if content_str != previous_content:
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        filename = f"{timestamp}.txt"
        has_error = any([row[5] == "ERROR" for row in result])
        if has_error:
            filename = f"{timestamp}-Error.txt"
        
        with open(filename, "w") as file:
            file.write(content_str)
        
        # Update previous results
        with open(file_name, "w") as file:
            file.write(content_str)

# prepare for connection 
jar_path = ""
url = ""
username = ""
password = ""
schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
