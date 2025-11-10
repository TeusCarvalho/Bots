import cv2
import mediapipe as mp
import math
from pycaw.pycaw import AudioUtilities, IAudioEndpointVolume
from ctypes import cast, POINTER
from comtypes import CLSCTX_ALL

# Inicializa o MediaPipe
mp_hands = mp.solutions.hands
hands = mp_hands.Hands(max_num_hands=1, min_detection_confidence=0.7)
mp_draw = mp.solutions.drawing_utils

# Inicializa o controle de volume do sistema (Windows)
devices = AudioUtilities.GetSpeakers()
interface = devices.Activate(IAudioEndpointVolume._iid_, CLSCTX_ALL, None)
volume = cast(interface, POINTER(IAudioEndpointVolume))
vol_min, vol_max = volume.GetVolumeRange()[:2]  # Range: (-65.25, 0.0)

# Abre a webcam
cap = cv2.VideoCapture(0)

while True:
    success, img = cap.read()
    img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    results = hands.process(img_rgb)

    if results.multi_hand_landmarks:
        for hand_landmarks in results.multi_hand_landmarks:
            mp_draw.draw_landmarks(img, hand_landmarks, mp_hands.HAND_CONNECTIONS)

            # Pontos: polegar (4) e indicador (8)
            x1, y1 = hand_landmarks.landmark[4].x, hand_landmarks.landmark[4].y
            x2, y2 = hand_landmarks.landmark[8].x, hand_landmarks.landmark[8].y

            # Converte para coordenadas da imagem
            h, w, c = img.shape
            x1, y1 = int(x1 * w), int(y1 * h)
            x2, y2 = int(x2 * w), int(y2 * h)

            # Calcula a distância entre os dedos
            dist = math.hypot(x2 - x1, y2 - y1)

            # Converte distância em volume
            vol = (dist - 20) * (vol_max - vol_min) / (200 - 20) + vol_min
            vol = max(min(vol, vol_max), vol_min)
            volume.SetMasterVolumeLevel(vol, None)

            # Exibe a barra de volume
            vol_bar = int((vol - vol_min) / (vol_max - vol_min) * 100)
            cv2.putText(img, f'Volume: {vol_bar}%', (30, 70), cv2.FONT_HERSHEY_SIMPLEX,
                        1, (0, 255, 0), 2)

            # Desenha linha entre os dedos
            cv2.line(img, (x1, y1), (x2, y2), (255, 0, 0), 3)
            cv2.circle(img, (x1, y1), 8, (0, 0, 255), -1)
            cv2.circle(img, (x2, y2), 8, (0, 0, 255), -1)

    cv2.imshow("Controle de Volume com a Mão", img)

    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()
