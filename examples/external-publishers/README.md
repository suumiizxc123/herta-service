# HERTA — External REST / Kafka publisher жишээ

Энэ хавтсыг өөр компьютер эсвэл өөр төсөл рүү хуулж, Electron аппын кодоос хамааралгүй ажиллуулна. **Хоёр connector:** REST болон Kafka. `source` нь REST connector-ийн API pull горимыг турших нэмэлт сервер юм.

```text
REST publisher ──POST──> Docker worker REST receiver ──> HERTA / inbox
REST source    <──GET─── Docker worker REST pull     ──> HERTA / inbox
Kafka publisher ──────> Kafka topic ──> Docker worker Kafka consumer ──> inbox
```

Бодит NIST transaction файлаа ашиглана. Эдгээр жишээ файл үүсгэх, ANSI-NIST стандарт хөрвүүлэх, biometric мэдээллийн хүчинтэй байдлыг шалгахгүй; үүнийг HERTA шийднэ. JSON дахь `fileContent` нь файлын бодит байтын Base64 юм.

## 1. Нэг удаагийн бэлтгэл

Node.js 22+ болон npm суусан байна. PowerShell:

```powershell
cd C:\herta_int\sumiya-service\examples\external-publishers
npm ci
Copy-Item .env.example .env
notepad .env
```

`Copy-Item`-ийг анхны тохиргоонд нэг удаа хийнэ. `.env`, NIST файл, нууц түлхүүр Git-д орохгүй. Жишээнүүдийн бүх тохиргоо энэ хавтасны `.env`-ээс уншигдана. NIST файлуудаа `C:\nist-input` гэх мэт репозиториос гаднах хавтаст хадгал.

## 2. REST publisher — гаднаас файл POST хийх

1. Аппын Dashboard-оос Docker worker ажиллаж байгааг шалга.
2. **Connectors → REST → POST receiver** тохируулж **Activate** хийнэ.
3. `.env` дээр receiver URL тохируулна:

```dotenv
REST_URL=http://127.0.0.1:18127/api/v1/nist
REST_TOKEN=
```

Суулгасан апп receiver token шаарддаг бол тухайн receiver-ийн token-ийг `REST_TOKEN`-д хийнэ. Token шаарддаггүй хувилбарт хоосон үлдээнэ. Энэ нь HERTA login token эсвэл worker management API token биш. Worker дахин асахад хуучин хувилбарын receiver token өөрчлөгдөж болно.

```powershell
npm run rest -- --file "C:\nist-input\subject-001.nist"
npm run rest -- --file "C:\nist-input\subject-001.nist" --file "C:\nist-input\subject-002.an2"
```

Хариуны утга:

| Утга | Тайлбар |
|---|---|
| `accepted`, `ids` | Worker хүлээн авсан файл, queue ID |
| `imported`, `results` байвал | Автомат import дэмждэг worker-ийн HERTA import үр дүн |
| HTTP `202` | Файлыг хүлээн авсан; HERTA import амжилттай болсон гэсэн баталгаа биш |
| HTTP `200`, `imported == accepted` | Автомат import дэмждэг worker бүх файлыг импортолсон гэж мэдээлсэн |

1.9.22 worker REST файлыг шууд HERTA руу импортлохыг оролдоно. HERTA session/импорт алдаатай бол файл inbox/history-д үлдэнэ. Өмнөх worker хувилбар файлыг inbox-д хадгалж хэрэглэгчээр review/import хийлгэнэ. Скрипт хоёр төрлийн хариуг дэмжинэ. Queue/history болон HERTA Subjects дээр үр дүнгээ шалга.

Бусад хэл дээрх системээс илгээх JSON гэрээ:

```json
{
  "files": [
    {
      "fileName": "subject-001.nist",
      "fileContent": "<бодит NIST файлын Base64>"
    }
  ]
}
```

`Content-Type: application/json`. Worker өөр өргөтгөлтэй оролтын нэрийг `.nist` болгож хэвийн болгох боловч файлын байтыг өөрчлөхгүй. 1–500 файл, файл бүр 1 byte–32 MiB, нийт 256 MiB хүртэл. Base64 дамжуулалтын хэмжээ ойролцоогоор 33% нэмэгдэнэ.

## 3. Kafka publisher — topic руу файл илгээх

Аппын local Kafka broker эсвэл өөрийн broker-ийг ажиллуулж, `herta.nist` topic-ийг урьдчилж үүсгэнэ. Publisher topic автоматаар үүсгэхгүй.

Аппын local Docker Kafka дээр topic үүсгэх PowerShell жишээ:

```powershell
docker exec herta-operations-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic herta.nist --partitions 3 --replication-factor 1
```

Container өөр нэртэй бол `docker ps`-оос Kafka container-ийн нэрийг шалгаж солино.

`.env`:

```dotenv
KAFKA_BROKERS=localhost:19092
KAFKA_TOPIC=herta.nist
KAFKA_FORMAT=json-base64
KAFKA_TLS=false
KAFKA_SASL_MECHANISM=
```

1. **Connectors → Kafka** дээр broker, topic, group, message format тохируулна.
2. Docker доторх worker аппын local Kafka-д `kafka:9092`-оор холбогдоно. Энэ хавтасны host дээрх publisher `localhost:19092` ашиглана.
3. Connector-оо **Activate** хийгээд consumer холбогдсоны дараа publish хийнэ. Шинэ consumer group өмнөх бүх message-ийг автоматаар уншихгүй (`fromBeginning: false`).

```powershell
npm run kafka -- --file "C:\nist-input\subject-001.nist"
npm run kafka -- --file "C:\nist-input\subject-001.nist" --file "C:\nist-input\subject-002.nist"
```

Файл бүр нэг message. Key нь файлын нэр; value нь REST-тэй ижил JSON envelope. Гаралт нь broker баталгаажуулсан topic, partition, offset-ийг харуулна. **Broker ACK нь HERTA import дууссан гэсэн үг биш.** Аппын Kafka receiver inbox-д review/import хийнэ; worker import/discard хийсний дараа offset commit хийнэ.

Raw NIST илгээх бол `.env`-д `KAFKA_FORMAT=raw-nist`, аппын Kafka connector дээр **Raw NIST bytes** сонгоно. Ижил publish команд ашиглана; message key файлын нэр, value бодит bytes байна.

Гаднын broker ашиглавал `KAFKA_BROKERS=broker.example.internal:9093` гэх мэт тохируулна. Publisher болон Docker worker хоёулаа broker-ийн `advertised.listeners` хаягуудад хүрдэг байх ёстой. SASL/TLS жишээ:

```dotenv
KAFKA_TLS=true
KAFKA_CA_FILE=C:/certificates/company-ca.pem
KAFKA_SASL_MECHANISM=scram-sha-512
KAFKA_USERNAME=publisher-user
KAFKA_PASSWORD=replace-with-real-password
```

Системийн итгэмжлэгдсэн CA ашиглаж байгаа бол `KAFKA_CA_FILE`-ийг хоосон үлдээнэ. Сертификатын шалгалт үргэлж асаалттай.

`KAFKA_MAX_MESSAGE_BYTES=900000` нь жишээний урьдчилсан хэмжээний хязгаар. JSON/Base64 үед ойролцоогоор 675 KB-аас жижиг файл багтана. Том NIST-д broker/topic message хэмжээ болон consumer fetch хязгаарыг хамтад нь тааруулсны дараа үүнийг өсгөнө, эсвэл REST ашиглана. 32 MiB NIST хязгаар нь Kafka-ийн default message limit-ийг өсгөхгүй.

## 4. REST API source — worker өөрөө татах жишээ

`.env` дээр:

```dotenv
SOURCE_HOST=0.0.0.0
SOURCE_PORT=18200
```

```powershell
npm run source -- --file "C:\nist-input\subject-001.nist"
```

Апп дээр **Connectors → REST → API pull** сонгоод URL:

```text
http://host.docker.internal:18200/nist
```

Docker worker host компьютер дээрх серверт энэ хаягаар хүрнэ. Өөр компьютер дээр сервер ажиллавал тухайн компьютерийн LAN IP/DNS ашиглана. **Test → Save → Activate** хийнэ. API нь GET бүрт файлыг дахин уншиж ижил JSON envelope буцаана; файлыг устгахгүй. Worker ижил нэр, байттай файлыг давхар татахыг шүүнэ. Шинэ файл нэмэхдээ серверээ шинэ `--file` аргументтай дахин асаана.

Энэ source server нь auth-гүй туршилтын жишээ. `0.0.0.0` нь LAN-д сонсох тул firewall-аар зөвхөн worker-ийн хандалтыг зөвшөөрөх эсвэл хамгаалалттай gateway-ийн цаана ажиллуул. GET `/nist`-ээс бусад зам 404 буцаана. Ctrl+C-ээр зогсооно.

## 5. Алдаа ба хүлээн авалтын шалгалт

| Шинж | Шалгах зүйл |
|---|---|
| REST 401 | Receiver token шаарддаг суулгасан хувилбарын token зөв эсэх |
| REST 409 | REST receiver Active эсэх |
| REST холбогдохгүй | Docker worker, порт `18127`, URL, firewall |
| `accepted > imported` | HERTA login/session, group, NIST format; receiver history/result |
| Kafka publish алдаа | Topic байгаа эсэх, broker address/advertised listeners, ACL, TLS/SASL, хэмжээ |
| Kafka ACK боловч inbox хоосон | Consumer Active, topic/format таарч байгаа эсэх, шинэ group publish-ээс өмнө холбогдсон эсэх |
| REST pull холбогдохгүй | `SOURCE_HOST=0.0.0.0`, `host.docker.internal`, firewall |

Timeout эсвэл acknowledgement алдагдсан үед message аль хэдийн хүрсэн байж болно. Publisher автоматаар REST retry хийхгүй. Дахин ажиллуулахын өмнө inbox/history шалга: receiver-д нийтлэг idempotency key гэрээ байхгүй, ижил файлыг дахин publish хийхэд давхардал үүсч болно. Kafka producer нэг process дотор idempotent боловч CLI-г дахин ажиллуулах нь шинэ publish юм.

Default Compose нь receiver `18127`, local Kafka `19092` портыг зөвхөн host loopback-д нээнэ. **Өөр компьютерээс default `127.0.0.1` хаягт хүрэхгүй.** Энэ хавтас port binding өөрчлөхгүй. Гаднын компьютерээс ашиглахад сүлжээнд хүрдэг broker болон authentication/TLS-тэй хамгаалсан REST gateway endpoint тохируулна.

## 6. Жишээ кодыг шалгах

```powershell
npm test
```

HTTP дамжуулалт, Base64/raw bytes, Kafka producer contract, хэмжээ/тохиргооны validation болон disconnect-ийг шалгана. Репозиторийн дотор ажиллуулбал тусгаарласан worker дээр REST end-to-end шалгалт нэмэгдэнэ; хавтсыг дангаар хуулсан бол worker шалгалтыг алгасана. Test fixture нь зөвхөн transport шалгах хиймэл bytes, HERTA-д импортлох бодит NIST биш. Live Kafka/HERTA шалгалтад өөрийн сервер, хүчинтэй NIST файл шаардлагатай.
