# HERTA Operations 1.9.22 — REST ба Kafka ашиглах заавар

## Шинэ хувилбарт шилжих

1. HERTA Operations Setup 1.9.22.exe суулгана.
2. HERTA хэрэглэгчээр нэг удаа дахин нэвтэрнэ. Хуучин session-д нууц үг хадгалагдаагүй тул token автоматаар шинэчлэхэд энэ алхам хэрэгтэй.
3. Dashboard → Apply / start дарж Docker worker image-ийг шинэчилнэ.
4. Worker online болсон эсэхийг шалгана.

Electron нь удирдлагын интерфэйс. Worker нь REST, Kafka, queue болон backup-ийг ажиллуулна. Цонхыг хаасан ч Docker ажиллаж байвал worker үргэлжилнэ. Компьютер sleep болсон үед ажиллахгүй.

## REST connector: API-аас татах

1. Connectors → Add REST сонгоно.
2. Pull from an API URL горим сонгоно.
3. API URL болон HERTA destination group оруулна. Default group нь 0.
4. Save connector → Activate дарна.
5. Test нь API хариуны файлыг шалгана. Fetch now нь өгөгдөл татаж HERTA руу шууд импортлоно.

REST connector auth шаардахгүй. Идэвхтэй polling connector үндсэндээ 60 секунд тутам API-г шалгана. Ижил нэр, ижил өгөгдөлтэй файлыг дахин татахгүй. HERTA импорт алдаатай бол файл queue-д үлдэнэ; Received inbox-оос шалгаж дахин импортлож болно.

API нь NIST файл агуулсан JSON envelope буцаана:

```json
{"fileName":"subject.nist","fileContent":"BASE64_NIST_BYTES"}
```

Энэ нь дурын subject JSON-ийг автоматаар NIST болгон хөрвүүлэх API биш. Бодит NIST файлын byte-уудыг Base64 болгоно.

## REST connector: POST хүсэлт хүлээн авах

1. Connectors → Add REST → Receive POST requests сонгоно.
2. HERTA destination group тохируулна.
3. Save connector → Activate дарна.
4. REST receiver URL хэсгээс endpoint авна.

Default endpoint: `http://127.0.0.1:18127/api/v1/nist`.
Authorization header эсвэл receiver token хэрэггүй.

```powershell
$file = 'C:\NIST\subject.nist'
$body = @{
  fileName = [IO.Path]::GetFileName($file)
  fileContent = [Convert]::ToBase64String([IO.File]::ReadAllBytes($file))
} | ConvertTo-Json
Invoke-RestMethod -Uri 'http://127.0.0.1:18127/api/v1/nist' -Method Post -ContentType 'application/json' -Body $body
```

Worker өгөгдлийг хадгалаад HERTA import API руу шууд илгээнэ:

- HTTP 200: бүх файлын импортын хүсэлт амжилттай.
- HTTP 202: хүлээн авсан боловч зарим файл импортлогдоогүй; queue-д үлдэнэ.
- `accepted`: хүлээн авсан файлын тоо.
- `imported`: HERTA амжилттай хүлээн авсан файлын тоо.
- `results`: файл тус бүрийн imported болон error мэдээлэл.
- Receiver идэвхгүй бол HTTP 409 буцаана.

Default Docker port зөвхөн тухайн компьютерийн localhost дээр нээлттэй. Өөр компьютерээс хандах бол сүлжээний нийтлэлтийг тусад нь тохируулна.

## Kafka connector

1. Connectors → Add Kafka сонгоно.
2. Broker, topic, consumer group оруулна.
3. Broker шаардвал TLS/SASL тохируулна.
4. Save connector → Activate дарна.
5. Received inbox эсвэл Import → Kafka хэсгээс ирсэн файлыг сонгож, preview хийгээд импортлоно.

REST болон Kafka нь тусдаа connector хэвээр. Энэ хувилбарын REST шууд импортын өөрчлөлт Kafka-ийн оператороор баталгаажуулах урсгалыг өөрчлөөгүй. Kafka broker-ийн өөрийн authentication шаардлагыг тохируулна.

## HERTA token автоматаар шинэчлэх

Worker нь HERTA-д login API ашиглан 1 цаг тутам дахин нэвтэрч token шинэчилнэ. Backup болон desktop-ийн worker-ээр дамжих HERTA үйлдлүүд нэг session ашиглана. HERTA 401/403 буцаавал дахин нэвтрээд хүсэлтийг нэг удаа давтана.

HERTA хэрэглэгчийн нэр, нууц үг desktop дээр Windows safeStorage, worker дээр AES-GCM шифрлэлттэй хадгалагдана. Sign out хийвэл хадгалсан session болон credentials арилна. Нууц үг өөрчлөгдвөл апп дээр дахин нэвтэрнэ. Worker API token-ийг соливол worker-ийн хуучин credentials тайлагдахгүй тул дахин нэвтэрнэ.

Worker ажиллахгүй үед desktop-ийн HERTA үйлдэл session-оо өөрөө шинэчилж ажиллана. Тасалдсан импортын network алдааг автоматаар давтаж давхар subject үүсгэхгүй; queue-ээс үр дүнг шалгана.

## Backup ба хяналт

Backup & restore дээр destination, frequency, retention сонгоод Save schedule дарна. Every minute нь туршилтын хуваарь. Хуваарь Docker worker дээр ажиллана.

Connector logs дээр холболтын үйл явдал, Queue history дээр хүлээн авалт болон импортын үр дүнг харна. Шууд импорт амжилттай болсон файл inbox-оос арилна; алдаатай файл error мэдээлэлтэй үлдэнэ.

## Шалгалтын хүрээ

Энэ хувилбарт 22 автомат тест болон REST/Kafka UI smoke шалгалт амжилттай. Auth-гүй REST intake, HERTA token шинэчлэлт, credentials шифрлэлт mock HERTA серверээр шалгагдсан. Бодит HERTA/Kafka сервер болон Docker runtime дээрх ажиллагааг тухайн орчинд шалгана.
