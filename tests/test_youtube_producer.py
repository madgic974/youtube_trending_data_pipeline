import pytest
from unittest.mock import MagicMock, patch

# Comme le code de `youtube_producer.py` n'est pas disponible, ces tests sont basés sur
# sa structure présumée. Nous supposons qu'il contient des fonctions comme `fetch_trending_videos`,
# `send_to_kafka`, et une fonction `main` qui orchestre le processus.
# Vous devrez probablement adapter les imports et les noms de fonctions pour qu'ils correspondent à votre code.
try:
    from ingestion import youtube_producer
except ImportError:
    # Créer des fonctions factices si le module ou les fonctions n'existent pas,
    # ce qui permet au fichier de test d'être analysé sans erreur.
    class DummyProducerModule:
        def fetch_trending_videos(self, *args, **kwargs): pass
        def send_to_kafka(self, *args, **kwargs): pass
        def main(self, *args, **kwargs): pass
    youtube_producer = DummyProducerModule()


# Données d'exemple qui imitent la réponse de l'API YouTube
FAKE_API_RESPONSE = {
    "items": [
        {"id": "video1", "snippet": {"title": "Title 1"}},
        {"id": "video2", "snippet": {"title": "Title 2"}},
    ]
}

# Données d'exemple pour une seule vidéo
FAKE_VIDEO_DATA = {"id": "video1", "snippet": {"title": "Title 1"}}


def test_fetch_trending_videos(mocker):
    """
    Teste la fonction de récupération des vidéos.
    Elle simule le client de l'API YouTube pour retourner une réponse prédéfinie
    et vérifie que la fonction traite correctement cette réponse.
    """
    # Simuler l'objet de service de l'API YouTube
    mock_youtube_service = MagicMock()
    mock_youtube_service.videos().list().execute.return_value = FAKE_API_RESPONSE

    # Nous supposons que votre code a une fonction comme 'fetch_trending_videos'
    # qui prend le client de service en argument.
    videos = youtube_producer.fetch_trending_videos(mock_youtube_service)

    # Vérifier que la fonction retourne les 'items' de la fausse réponse
    assert len(videos) == 2
    assert videos == FAKE_API_RESPONSE["items"]
    mock_youtube_service.videos().list.assert_called_once()


def test_send_to_kafka(mocker):
    """
    Teste la fonction d'envoi à Kafka.
    Elle simule le KafkaProducer et vérifie que sa méthode 'send'
    est appelée avec le bon topic et les bonnes données.
    """
    mock_kafka_producer = MagicMock()
    youtube_producer.send_to_kafka(mock_kafka_producer, "test-topic", FAKE_VIDEO_DATA)
    mock_kafka_producer.send.assert_called_once_with("test-topic", FAKE_VIDEO_DATA)


@patch('ingestion.youtube_producer.get_youtube_service')
@patch('ingestion.youtube_producer.get_kafka_producer')
@patch('ingestion.youtube_producer.fetch_trending_videos', return_value=FAKE_API_RESPONSE['items'])
@patch('ingestion.youtube_producer.send_to_kafka')
@patch('time.sleep', return_value=None)
def test_main_loop_integration(mock_sleep, mock_send_kafka, mock_fetch, mock_get_kafka, mock_get_youtube):
    """
    Teste la boucle d'exécution principale du producteur en tant que test d'intégration.
    Pour éviter une boucle infinie pendant le test, nous levons une exception après la première exécution.
    """
    mock_send_kafka.side_effect = StopIteration
    with pytest.raises(StopIteration):
        youtube_producer.main()

    mock_get_youtube.assert_called_once()
    mock_get_kafka.assert_called_once()
    mock_fetch.assert_called_once()
    mock_send_kafka.assert_called_once()