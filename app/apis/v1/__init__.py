from fastapi import APIRouter, Depends

router = APIRouter()


from . import system
from . import chat