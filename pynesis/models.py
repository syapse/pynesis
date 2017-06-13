from django.db import models


class Checkpoint(models.Model):
    key = models.CharField(max_length=255, primary_key=True)
    checkpoints = models.TextField()

    class Meta:
        app_label = "pynesis"
