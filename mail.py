import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formatdate


from_address = 'errormail@xxxxxx.co.jp'
to_address = ['haruyama.y@xxxxxx.co.jp','','']

charset = 'ISO-2022-JP'
subject = 'エラー発生のためシステムを停止しました。'
text = 'システムを停止しました。エラーログを確認して再起動してください。'

msg = MIMEText(text, 'plain', charset)
msg['Subject'] = Header(subject, charset)
msg['From'] = from_address
msg['To'] = ",".join(to_address)
msg['Date'] = formatdate(localtime=True)

def to_mail():
    smtp = smtplib.SMTP('localhost')
    out =smtp.sendmail(from_address, to_address, msg.as_string())
    smtp.quit()

# to_mail()